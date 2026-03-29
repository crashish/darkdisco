import type { Client, Institution, WatchTerm, Finding, FindingDetail, Source, DashboardStats, FindingStatus, Severity, TelegramChannel, DiscordGuildChannel, PollTriggerResult, FindingTrend, RawMention, PaginatedFindings, AuditLogEntry, ReportRequest, ReportTemplate, ReportTemplateConfig, ReportSchedule, GeneratedReport, DateRangeMode, DeliveryMethod, BINLookupResult, BINRecord, BINStats, BINImportResult, ThreatSummary, DispositionAnalytics, AlertRule, AlertRuleCreate, AlertRuleUpdate, Notification } from './types';
import {
  mockClients, mockInstitutions, mockWatchTerms, mockFindings, mockFindingDetails,
  mockSources, mockDashboardStats, mockRawMentions,
} from './mockData';

const BASE = '/api';

async function apiFetch<T>(url: string, fallback: T, init?: RequestInit): Promise<T> {
  const token = localStorage.getItem('dd_token');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const res = await fetch(`${BASE}${url}`, {
    headers,
    ...init,
  });
  if (res.status === 401) {
    localStorage.removeItem('dd_token');
    window.dispatchEvent(new CustomEvent('auth:logout', { detail: 'unauthorized' }));
    throw new Error('Unauthorized');
  }
  if (!res.ok) throw new Error(`${res.status}`);
  return await res.json() as T;
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

export async function fetchCurrentUser(): Promise<{ id: string; username: string; role: string }> {
  return apiFetch('/auth/me', { id: '', username: '', role: '' });
}

export async function changePassword(currentPassword: string, newPassword: string): Promise<{ status: string; message: string }> {
  return apiFetch('/auth/change-password', { status: '', message: '' }, {
    method: 'POST',
    body: JSON.stringify({ current_password: currentPassword, new_password: newPassword }),
  });
}

export async function fetchDashboardStats(): Promise<DashboardStats> {
  return apiFetch('/dashboard/stats', mockDashboardStats);
}

export async function fetchClients(): Promise<Client[]> {
  return apiFetch('/clients', mockClients);
}

export async function fetchInstitutions(clientId?: string): Promise<Institution[]> {
  const qs = clientId ? `?client_id=${clientId}` : '';
  const fallback = clientId
    ? mockInstitutions.filter(i => i.client_id === clientId)
    : mockInstitutions;
  return apiFetch(`/institutions${qs}`, fallback);
}

export async function fetchFindings(params?: {
  institution_id?: string;
  severity?: string;
  status?: string;
  date_from?: string;
  date_to?: string;
  q?: string;
  page?: number;
  page_size?: number;
}): Promise<PaginatedFindings> {
  const qs = new URLSearchParams();
  if (params?.institution_id) qs.set('institution_id', params.institution_id);
  if (params?.severity) qs.set('severity', params.severity);
  if (params?.status) qs.set('status', params.status);
  if (params?.date_from) qs.set('date_from', params.date_from);
  if (params?.date_to) qs.set('date_to', params.date_to);
  if (params?.q) qs.set('q', params.q);
  if (params?.page !== undefined) qs.set('page', String(params.page));
  if (params?.page_size !== undefined) qs.set('page_size', String(params.page_size));
  const q = qs.toString();

  const fallback: PaginatedFindings = { items: mockFindings, total: mockFindings.length, page: params?.page ?? 1, page_size: params?.page_size ?? 50 };
  return apiFetch(`/findings${q ? '?' + q : ''}`, fallback);
}

export async function fetchFinding(id: string): Promise<FindingDetail> {
  const fallback = mockFindingDetails.find(f => f.id === id) || mockFindingDetails[0];
  return apiFetch(`/findings/${id}`, fallback);
}

export async function updateFindingStatus(id: string, status: FindingStatus): Promise<Finding> {
  const fallback = { ...mockFindings.find(f => f.id === id)!, status };
  return apiFetch(`/findings/${id}/transition`, fallback, {
    method: 'POST',
    body: JSON.stringify({ status }),
  });
}

export async function updateFinding(id: string, body: {
  severity?: Severity;
  classification?: string;
  analyst_notes?: string;
}): Promise<FindingDetail> {
  return apiFetch(`/findings/${id}`, {} as FindingDetail, {
    method: 'PUT',
    body: JSON.stringify(body),
  });
}

export async function addFindingNote(id: string, content: string): Promise<FindingDetail> {
  return apiFetch(`/findings/${id}/notes`, {} as FindingDetail, {
    method: 'POST',
    body: JSON.stringify({ content }),
  });
}

export async function fetchAuditLog(findingId: string): Promise<AuditLogEntry[]> {
  return apiFetch(`/findings/${findingId}/audit-log`, []);
}

export async function fetchClassifications(): Promise<string[]> {
  return apiFetch('/findings/classifications', []);
}

export async function fetchWatchTerms(institutionId: string): Promise<WatchTerm[]> {
  const fallback = mockWatchTerms.filter(w => w.institution_id === institutionId);
  return apiFetch(`/watch-terms?institution_id=${institutionId}`, fallback);
}

export async function createInstitution(body: { client_id: string; name: string; city: string; state: string; charter_number?: string }): Promise<Institution> {
  return apiFetch('/institutions', {} as Institution, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function updateInstitution(id: string, body: Partial<{ name: string; city: string; state: string; charter_number: string }>): Promise<Institution> {
  return apiFetch(`/institutions/${id}`, {} as Institution, {
    method: 'PUT',
    body: JSON.stringify(body),
  });
}

export async function deleteInstitution(id: string): Promise<void> {
  await apiFetch(`/institutions/${id}`, null, { method: 'DELETE' });
}

export async function createWatchTerm(body: { institution_id: string; term_type: string; value: string }): Promise<WatchTerm> {
  return apiFetch('/watch-terms', {} as WatchTerm, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function deleteWatchTerm(id: string): Promise<void> {
  await apiFetch(`/watch-terms/${id}`, null, { method: 'DELETE' });
}

export function getInstitutionExportUrl(format: 'json' | 'csv', clientId?: string): string {
  const params = new URLSearchParams({ format });
  if (clientId) params.set('client_id', clientId);
  return `${BASE}/institutions/export?${params}`;
}

export async function importInstitutions(file: File, clientId: string): Promise<{ imported: number; skipped: number; errors: string[] }> {
  const token = localStorage.getItem('dd_token');
  const formData = new FormData();
  formData.append('file', file);
  const res = await fetch(`${BASE}/institutions/import?client_id=${encodeURIComponent(clientId)}`, {
    method: 'POST',
    headers: token ? { 'Authorization': `Bearer ${token}` } : {},
    body: formData,
  });
  if (res.status === 401) {
    localStorage.removeItem('dd_token');
    window.dispatchEvent(new CustomEvent('auth:logout', { detail: 'unauthorized' }));
    throw new Error('Unauthorized');
  }
  if (!res.ok) throw new Error(`${res.status}`);
  return await res.json();
}

export async function fetchSources(enabled?: boolean): Promise<Source[]> {
  const qs = enabled !== undefined ? `?enabled=${enabled}` : '';
  return apiFetch(`/sources${qs}`, mockSources);
}

export async function fetchSource(id: string): Promise<Source> {
  const fallback = mockSources.find(s => s.id === id) || mockSources[0];
  return apiFetch(`/sources/${id}`, fallback);
}

export async function fetchChannels(sourceId: string): Promise<TelegramChannel[]> {
  return apiFetch(`/sources/${sourceId}/channels`, []);
}

export async function addChannel(sourceId: string, channel: string, join: boolean = true): Promise<TelegramChannel> {
  return apiFetch(`/sources/${sourceId}/channels`, { channel, last_message_id: null }, {
    method: 'POST',
    body: JSON.stringify({ channel, join }),
  });
}

export async function removeChannel(sourceId: string, channel: string): Promise<{ removed: string }> {
  return apiFetch(`/sources/${sourceId}/channels/${encodeURIComponent(channel)}`, { removed: channel }, {
    method: 'DELETE',
  });
}

export async function updateSource(sourceId: string, body: Record<string, unknown>): Promise<Source> {
  const fallback = mockSources.find(s => s.id === sourceId) || mockSources[0];
  return apiFetch(`/sources/${sourceId}`, fallback, {
    method: 'PUT',
    body: JSON.stringify(body),
  });
}

export async function triggerPoll(sourceId: string): Promise<PollTriggerResult> {
  return apiFetch(`/sources/${sourceId}/poll`, { status: 'dispatched', task_id: 'mock', source_id: sourceId }, {
    method: 'POST',
  });
}

export async function fetchSourceFindings(sourceId: string): Promise<Finding[]> {
  return apiFetch(`/sources/${sourceId}/findings`, mockFindings.slice(0, 5));
}

export async function fetchSourceFindingsTrend(sourceId: string, days: number = 14): Promise<FindingTrend[]> {
  const mockTrend = Array.from({ length: days }, (_, i) => ({
    date: new Date(Date.now() - (days - 1 - i) * 86400000).toISOString().split('T')[0],
    count: Math.floor(Math.random() * 6) + 1,
  }));
  return apiFetch(`/sources/${sourceId}/findings/trend?days=${days}`, mockTrend);
}

export async function fetchDiscordChannels(sourceId: string): Promise<DiscordGuildChannel[]> {
  return apiFetch(`/sources/${sourceId}/discord-channels`, []);
}

export async function addDiscordChannel(sourceId: string, guildId: string, channelId: string): Promise<DiscordGuildChannel> {
  return apiFetch(`/sources/${sourceId}/discord-channels`, { guild_id: guildId, channel_ids: [channelId] }, {
    method: 'POST',
    body: JSON.stringify({ guild_id: guildId, channel_id: channelId }),
  });
}

export async function removeDiscordChannel(sourceId: string, guildId: string, channelId: string): Promise<{ guild_id: string; removed_channel: string }> {
  return apiFetch(`/sources/${sourceId}/discord-channels/${guildId}/${channelId}`, { guild_id: guildId, removed_channel: channelId }, {
    method: 'DELETE',
  });
}

export interface PaginatedMentions {
  items: RawMention[];
  total: number;
  page: number;
  page_size: number;
}

export async function fetchMentions(params?: {
  source_id?: string;
  source_ids?: string;
  source_type?: string;
  promoted?: boolean;
  channel?: string;
  channels?: string;
  has_media?: boolean;
  q?: string;
  sort_by?: string;
  sort_dir?: string;
  date_from?: string;
  date_to?: string;
  page?: number;
  page_size?: number;
}): Promise<PaginatedMentions> {
  const qs = new URLSearchParams();
  if (params?.source_id) qs.set('source_id', params.source_id);
  if (params?.source_ids) qs.set('source_ids', params.source_ids);
  if (params?.source_type) qs.set('source_type', params.source_type);
  if (params?.promoted !== undefined) qs.set('promoted', String(params.promoted));
  if (params?.channel) qs.set('channel', params.channel);
  if (params?.channels) qs.set('channels', params.channels);
  if (params?.has_media !== undefined) qs.set('has_media', String(params.has_media));
  if (params?.q) qs.set('q', params.q);
  if (params?.sort_by) qs.set('sort_by', params.sort_by);
  if (params?.sort_dir) qs.set('sort_dir', params.sort_dir);
  if (params?.date_from) qs.set('date_from', params.date_from);
  if (params?.date_to) qs.set('date_to', params.date_to);
  if (params?.page !== undefined) qs.set('page', String(params.page));
  if (params?.page_size !== undefined) qs.set('page_size', String(params.page_size));
  const q = qs.toString();

  let fallback = [...mockRawMentions];
  if (params?.source_id) fallback = fallback.filter(m => m.source_id === params.source_id);
  if (params?.promoted !== undefined) {
    fallback = params.promoted
      ? fallback.filter(m => m.promoted_to_finding_id != null)
      : fallback.filter(m => !m.promoted_to_finding_id);
  }

  return apiFetch(`/mentions${q ? '?' + q : ''}`, { items: fallback, total: fallback.length, page: params?.page ?? 1, page_size: params?.page_size ?? 50 });
}

export async function fetchMentionChannels(): Promise<{ channel: string; count: number }[]> {
  return apiFetch('/mentions/channels', []);
}

export interface MentionFileInfo {
  type: 'original' | 'extracted';
  filename: string;
  size: number | null;
  mime?: string;
  extension?: string;
  sha256?: string;
  s3_key: string | null;
  download_url: string | null;
}

export interface MentionFilesResponse {
  mention_id: string;
  original_file: string | null;
  download_status: string | null;
  passwords: string[];
  has_credentials: boolean;
  credential_count: number;
  credential_samples: string[];
  files: MentionFileInfo[];
}

export async function fetchMentionFiles(mentionId: string): Promise<MentionFilesResponse> {
  return apiFetch(`/mentions/${mentionId}/files`, {
    mention_id: mentionId, original_file: null, download_status: null,
    passwords: [], has_credentials: false, credential_count: 0,
    credential_samples: [], files: [],
  });
}

export function getMentionFileUrl(mentionId: string): string {
  return `/api/mentions/${mentionId}/file`;
}

export function getS3FileUrl(s3Key: string): string {
  return `/api/files/${s3Key}`;
}

export async function generateReportPdf(body: ReportRequest): Promise<Blob> {
  const token = localStorage.getItem('dd_token');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const res = await fetch(`${BASE}/reports/generate`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    localStorage.removeItem('dd_token');
    window.dispatchEvent(new CustomEvent('auth:logout', { detail: 'unauthorized' }));
    throw new Error('Unauthorized');
  }
  if (!res.ok) throw new Error(`${res.status}`);
  return await res.blob();
}

export async function previewReportHtml(body: ReportRequest): Promise<string> {
  const token = localStorage.getItem('dd_token');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const res = await fetch(`${BASE}/reports/preview`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    localStorage.removeItem('dd_token');
    window.dispatchEvent(new CustomEvent('auth:logout', { detail: 'unauthorized' }));
    throw new Error('Unauthorized');
  }
  if (!res.ok) throw new Error(`${res.status}`);
  return await res.text();
}

export async function fetchArchiveContents(
  type: 'mentions' | 'findings',
  id: string,
): Promise<{ files: { filename: string; size: number; preview: string; content: string }[]; total: number }> {
  return apiFetch(`/${type}/${id}/archive-contents`, { files: [], total: 0 });
}

export interface OcrStats {
  total_cached: number;
  mentions_with_ocr: number;
  avg_confidence: number;
  recent: {
    sha256: string;
    text_preview: string;
    confidence: number;
    engine: string;
    created_at: string | null;
  }[];
}

export async function fetchOcrStats(): Promise<OcrStats> {
  return apiFetch('/ocr-stats', { total_cached: 0, mentions_with_ocr: 0, avg_confidence: 0, recent: [] });
}

export interface OcrProcessResult {
  text: string;
  confidence: number;
  engine: string;
  cached: boolean;
  error?: string;
}

export async function processOcr(s3Key: string, sha256?: string): Promise<OcrProcessResult> {
  const body: Record<string, string> = { s3_key: s3Key };
  if (sha256) body.sha256 = sha256;
  return apiFetch('/ocr/process', { text: '', confidence: 0, engine: 'none', cached: false }, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function promoteMention(mentionId: string, body: {
  institution_id: string;
  title: string;
  severity?: string;
  summary?: string;
  tags?: string[];
}): Promise<Finding> {
  return apiFetch(`/mentions/${mentionId}/promote`, mockFindings[0], {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

// ---------------------------------------------------------------------------
// Report Templates
// ---------------------------------------------------------------------------

export async function fetchReportTemplates(): Promise<ReportTemplate[]> {
  return apiFetch('/reports/templates', []);
}

export async function createReportTemplate(name: string, description: string | null, config: ReportTemplateConfig): Promise<ReportTemplate> {
  return apiFetch('/reports/templates', {} as ReportTemplate, {
    method: 'POST',
    body: JSON.stringify({ name, description, config }),
  });
}

export async function updateReportTemplate(id: string, data: { name?: string; description?: string | null; config?: ReportTemplateConfig }): Promise<ReportTemplate> {
  return apiFetch(`/reports/templates/${id}`, {} as ReportTemplate, {
    method: 'PUT',
    body: JSON.stringify(data),
  });
}

export async function deleteReportTemplate(id: string): Promise<void> {
  const token = localStorage.getItem('dd_token');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const res = await fetch(`${BASE}/reports/templates/${id}`, { method: 'DELETE', headers });
  if (res.status === 401) {
    localStorage.removeItem('dd_token');
    window.dispatchEvent(new CustomEvent('auth:logout', { detail: 'unauthorized' }));
    throw new Error('Unauthorized');
  }
  if (!res.ok) throw new Error(`${res.status}`);
}

// ---------------------------------------------------------------------------
// Report Schedules
// ---------------------------------------------------------------------------

export async function fetchReportSchedules(): Promise<ReportSchedule[]> {
  return apiFetch('/reports/schedules', []);
}

export async function createReportSchedule(data: {
  template_id: string;
  name: string;
  cron_expression?: string;
  interval_seconds?: number;
  date_range_mode?: DateRangeMode;
  enabled?: boolean;
  delivery_method?: DeliveryMethod;
  recipients?: string[];
}): Promise<ReportSchedule> {
  return apiFetch('/reports/schedules', {} as ReportSchedule, {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function updateReportSchedule(id: string, data: Partial<{
  name: string;
  template_id: string;
  cron_expression: string;
  interval_seconds: number;
  date_range_mode: DateRangeMode;
  enabled: boolean;
  delivery_method: DeliveryMethod;
  recipients: string[];
}>): Promise<ReportSchedule> {
  return apiFetch(`/reports/schedules/${id}`, {} as ReportSchedule, {
    method: 'PUT',
    body: JSON.stringify(data),
  });
}

export async function deleteReportSchedule(id: string): Promise<void> {
  const token = localStorage.getItem('dd_token');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const res = await fetch(`${BASE}/reports/schedules/${id}`, { method: 'DELETE', headers });
  if (res.status === 401) {
    localStorage.removeItem('dd_token');
    window.dispatchEvent(new CustomEvent('auth:logout', { detail: 'unauthorized' }));
    throw new Error('Unauthorized');
  }
  if (!res.ok) throw new Error(`${res.status}`);
}

// ---------------------------------------------------------------------------
// Generated Reports
// ---------------------------------------------------------------------------

export async function fetchGeneratedReports(scheduleId?: string): Promise<GeneratedReport[]> {
  const qs = scheduleId ? `?schedule_id=${scheduleId}` : '';
  return apiFetch(`/reports/generated${qs}`, []);
}

export async function downloadGeneratedReport(id: string): Promise<void> {
  const token = localStorage.getItem('dd_token');
  const headers: Record<string, string> = {};
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const res = await fetch(`${BASE}/reports/generated/${id}/download`, { headers });
  if (res.status === 401) {
    localStorage.removeItem('dd_token');
    window.dispatchEvent(new CustomEvent('auth:logout', { detail: 'unauthorized' }));
    return;
  }
  if (!res.ok) throw new Error(`${res.status}`);
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  const disp = res.headers.get('content-disposition');
  const match = disp?.match(/filename="?([^"]+)"?/);
  a.download = match?.[1] || 'report.pdf';
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// BIN Database
// ---------------------------------------------------------------------------

export async function lookupBIN(prefix: string): Promise<BINLookupResult> {
  return apiFetch(`/bins/lookup/${prefix}`, {
    bin_prefix: prefix, found: false, issuer_name: null, card_brand: null,
    card_type: null, card_level: null, country_code: null, country_name: null,
    bank_url: null, bank_phone: null,
  });
}

export async function searchBINs(params?: {
  q?: string;
  brand?: string;
  country?: string;
  limit?: number;
  offset?: number;
}): Promise<BINRecord[]> {
  const qs = new URLSearchParams();
  if (params?.q) qs.set('q', params.q);
  if (params?.brand) qs.set('brand', params.brand);
  if (params?.country) qs.set('country', params.country);
  if (params?.limit !== undefined) qs.set('limit', String(params.limit));
  if (params?.offset !== undefined) qs.set('offset', String(params.offset));
  const q = qs.toString();
  return apiFetch(`/bins/search${q ? '?' + q : ''}`, []);
}

export async function fetchBINStats(): Promise<BINStats> {
  return apiFetch('/bins/stats', {
    total_records: 0, by_brand: {}, by_source: {},
    by_country: [], top_issuers: [],
  });
}

export async function triggerRetroactiveHunt(institutionId: string, days?: number): Promise<{ status: string; institution: string; days: number | null }> {
  const body: Record<string, unknown> = {};
  if (days !== undefined) body.days = days;
  return apiFetch(`/institutions/${institutionId}/retroactive-hunt`, { status: 'dispatched', institution: '', days: null }, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

// ---------------------------------------------------------------------------
// Matching Filters
// ---------------------------------------------------------------------------

export interface MatchingFilters {
  fraud_indicators: string[];
  negative_patterns: string[];
}

export interface MatchingFiltersTestResult {
  matched_negative_patterns: string[];
  matched_fraud_indicators: string[];
  would_suppress: boolean;
  would_require_fraud_indicator: boolean;
}

export async function fetchMatchingFilters(): Promise<MatchingFilters> {
  return apiFetch('/settings/matching-filters', { fraud_indicators: [], negative_patterns: [] });
}

export async function updateMatchingFilters(data: MatchingFilters): Promise<MatchingFilters> {
  return apiFetch('/settings/matching-filters', { fraud_indicators: [], negative_patterns: [] }, {
    method: 'PUT',
    body: JSON.stringify(data),
  });
}

export async function testMatchingFilters(text: string): Promise<MatchingFiltersTestResult> {
  return apiFetch('/settings/matching-filters/test', {
    matched_negative_patterns: [],
    matched_fraud_indicators: [],
    would_suppress: false,
    would_require_fraud_indicator: false,
  }, {
    method: 'POST',
    body: JSON.stringify({ text }),
  });
}

export async function fetchThreatSummary(institutionId: string, days: number = 90): Promise<ThreatSummary> {
  return apiFetch(`/institutions/${institutionId}/threat-summary?days=${days}`, {
    institution_id: institutionId, institution_name: '', findings_timeline: [],
    threat_categories: [], total_findings: 0, confirmed_threats: 0,
    active_threat_actors: 0, top_source_channels: [], by_severity: {},
    by_status: {}, executive_brief: '',
  });
}

export async function fetchDispositionAnalytics(params?: {
  days?: number;
  institution_id?: string;
}): Promise<DispositionAnalytics> {
  const qs = new URLSearchParams();
  if (params?.days !== undefined) qs.set('days', String(params.days));
  if (params?.institution_id) qs.set('institution_id', params.institution_id);
  const query = qs.toString() ? `?${qs}` : '';
  return apiFetch(`/analytics/disposition${query}`, {
    institution_fp_rates: [],
    pattern_effectiveness: {
      total_mentions: 0, total_promoted: 0, total_suppressed: 0,
      suppression_rate: 0, fp_score_distribution: [],
    },
    analyst_workload: {
      pending_review: 0, avg_disposition_hours: null,
      disposition_breakdown: [], by_analyst: [],
    },
    disposition_trends: [],
  });
}

// ---------------------------------------------------------------------------
// Alert Rules
// ---------------------------------------------------------------------------

export async function fetchAlertRules(params?: {
  owner_id?: string;
  enabled?: boolean;
}): Promise<AlertRule[]> {
  const qs = new URLSearchParams();
  if (params?.owner_id) qs.set('owner_id', params.owner_id);
  if (params?.enabled !== undefined) qs.set('enabled', String(params.enabled));
  const q = qs.toString();
  return apiFetch(`/alert-rules${q ? '?' + q : ''}`, []);
}

export async function createAlertRule(body: AlertRuleCreate): Promise<AlertRule> {
  return apiFetch('/alert-rules', {} as AlertRule, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function updateAlertRule(id: string, body: AlertRuleUpdate): Promise<AlertRule> {
  return apiFetch(`/alert-rules/${id}`, {} as AlertRule, {
    method: 'PUT',
    body: JSON.stringify(body),
  });
}

export async function deleteAlertRule(id: string): Promise<void> {
  await apiFetch(`/alert-rules/${id}`, null, { method: 'DELETE' });
}

// ---------------------------------------------------------------------------
// Notifications (Alert History)
// ---------------------------------------------------------------------------

export interface PaginatedNotifications {
  items: Notification[];
  total: number;
  page: number;
  page_size: number;
}

export async function fetchNotifications(params?: {
  user_id?: string;
  unread_only?: boolean;
  page?: number;
  page_size?: number;
}): Promise<Notification[]> {
  const qs = new URLSearchParams();
  if (params?.user_id) qs.set('user_id', params.user_id);
  if (params?.unread_only !== undefined) qs.set('unread_only', String(params.unread_only));
  if (params?.page !== undefined) qs.set('page', String(params.page));
  if (params?.page_size !== undefined) qs.set('page_size', String(params.page_size));
  const q = qs.toString();
  return apiFetch(`/notifications${q ? '?' + q : ''}`, []);
}

export async function markNotificationRead(id: string): Promise<Notification> {
  return apiFetch(`/notifications/${id}/read`, {} as Notification, { method: 'PUT' });
}

export async function markAllNotificationsRead(): Promise<{ marked: number }> {
  return apiFetch('/notifications/mark-all-read', { marked: 0 }, { method: 'POST' });
}

// ---------------------------------------------------------------------------
// BIN Import
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// System Settings
// ---------------------------------------------------------------------------

export interface SystemSetting {
  key: string;
  value: string;
  description: string | null;
  updated_at: string | null;
}

export async function fetchSystemSettings(): Promise<SystemSetting[]> {
  return apiFetch('/settings/system', []);
}

export async function updateSystemSetting(key: string, value: string): Promise<SystemSetting> {
  return apiFetch(`/settings/system/${key}`, {} as SystemSetting, {
    method: 'PUT',
    body: JSON.stringify({ value }),
  });
}

// ---------------------------------------------------------------------------
// BIN Import
// ---------------------------------------------------------------------------

export async function importBINFile(file: File, sourceLabel: string = 'csv'): Promise<BINImportResult> {
  const token = localStorage.getItem('dd_token');
  const formData = new FormData();
  formData.append('file', file);
  const res = await fetch(`${BASE}/bins/import?source_label=${encodeURIComponent(sourceLabel)}`, {
    method: 'POST',
    headers: token ? { 'Authorization': `Bearer ${token}` } : {},
    body: formData,
  });
  if (res.status === 401) {
    localStorage.removeItem('dd_token');
    window.dispatchEvent(new CustomEvent('auth:logout', { detail: 'unauthorized' }));
    throw new Error('Unauthorized');
  }
  if (!res.ok) throw new Error(`${res.status}`);
  return await res.json();
}
