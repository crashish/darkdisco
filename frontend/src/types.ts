export type Severity = 'critical' | 'high' | 'medium' | 'low' | 'info';
export type FindingStatus = 'new' | 'reviewing' | 'escalated' | 'confirmed' | 'dismissed' | 'false_positive' | 'resolved';
export type SourceType = 'tor_forum' | 'paste_site' | 'telegram' | 'telegram_intel' | 'discord' | 'breach_db' | 'ransomware_blog' | 'forum' | 'marketplace' | 'stealer_log' | 'other';
export type SourceHealth = 'healthy' | 'degraded' | 'offline';

export interface Client {
  id: string;
  name: string;
  created_at: string;
}

export interface Institution {
  id: string;
  client_id: string;
  client_name?: string;
  name: string;
  city: string;
  state: string;
  charter_number?: string;
  created_at: string;
}

export interface WatchTerm {
  id: string;
  institution_id: string;
  term_type: 'name' | 'domain' | 'bin' | 'routing_number';
  value: string;
  created_at: string;
}

export interface Finding {
  id: string;
  institution_id: string;
  institution_name?: string;
  source_type: SourceType;
  severity: Severity;
  status: FindingStatus;
  title: string;
  summary: string;
  source_url?: string;
  discovered_at: string;
  updated_at: string;
  metadata?: Record<string, unknown> | null;
}

export interface PaginatedFindings {
  items: Finding[];
  total: number;
  page: number;
  page_size: number;
}

export interface FindingDetail extends Finding {
  raw_content: string | null;
  matched_terms: MatchedTerm[] | null;
  tags: string[] | null;
  classification: string | null;
  analyst_notes: string | null;
  enrichment: EnrichmentData | null;
  status_history: StatusHistoryEntry[];
  created_at: string;
  reviewed_by: string | null;
  reviewed_at: string | null;
  source_name?: string;
  metadata?: Record<string, unknown> | null;
}

export interface AuditLogEntry {
  id: string;
  finding_id: string;
  action: string;
  username: string | null;
  field: string | null;
  old_value: string | null;
  new_value: string | null;
  created_at: string;
}

export interface HighlightSpan {
  start: number;
  end: number;
}

export interface MatchedTerm {
  term_id: number;
  term_type: string;
  value: string;
  context?: string;
  highlights?: HighlightSpan[];
}

export interface EnrichmentData {
  dedup?: { similarity_score?: number; similar_finding_id?: string; action?: string };
  false_positive?: { is_fp?: boolean; confidence?: number; reason?: string };
  threat_intel?: Record<string, unknown>;
}

export interface StatusHistoryEntry {
  status: FindingStatus;
  changed_at: string;
  changed_by?: string;
  notes?: string;
}

export interface RawMention {
  id: string;
  source_id: string;
  source_name?: string;
  source_type?: SourceType;
  content: string;
  content_hash?: string;
  source_url?: string;
  metadata?: Record<string, unknown>;
  collected_at: string;
  promoted_to_finding_id?: string | null;
}

export interface Source {
  id: string;
  name: string;
  source_type: SourceType;
  health: SourceHealth;
  enabled: boolean;
  last_poll: string;
  finding_count: number;
  avg_poll_seconds: number;
  poll_interval_seconds: number;
  last_polled_at: string | null;
  last_error: string | null;
  config?: Record<string, unknown>;
}

export interface TelegramChannel {
  channel: string;
  last_message_id: number | null;
}

export interface DiscordGuildChannel {
  guild_id: string;
  channel_ids: string[];
}

export interface PollTriggerResult {
  status: string;
  task_id: string;
  source_id: string;
}

export interface FindingTrend {
  date: string;
  count: number;
}

export interface DashboardStats {
  total_findings: number;
  findings_by_severity: Record<Severity, number>;
  new_today: number;
  monitored_institutions: number;
  active_sources: number;
  findings_trend: { date: string; count: number }[];
}

export interface ReportSections {
  executive_summary: boolean;
  charts: boolean;
  findings_detail: boolean;
  findings_by_severity: boolean;
  source_activity: boolean;
  institution_exposure: boolean;
  classification_breakdown: boolean;
  timeline: boolean;
}

export interface ReportChartOptions {
  severity_pie: boolean;
  status_pie: boolean;
  trend_line: boolean;
  source_bar: boolean;
  institution_bar: boolean;
  severity_trend: boolean;
}

export interface ReportRequest {
  title: string;
  date_from?: string;
  date_to?: string;
  client_id?: string;
  institution_id?: string;
  severities?: string[];
  statuses?: string[];
  sections: ReportSections;
  charts: ReportChartOptions;
}

export interface ReportTemplateConfig {
  title: string;
  client_id?: string;
  institution_id?: string;
  severities?: string[];
  statuses?: string[];
  sections: ReportSections;
  charts: ReportChartOptions;
}

export interface ReportTemplate {
  id: string;
  name: string;
  description: string | null;
  owner_id: string;
  config: ReportTemplateConfig;
  created_at: string;
  updated_at: string;
}

export type DateRangeMode = 'last_24h' | 'last_7d' | 'last_30d' | 'last_quarter' | 'custom';
export type DeliveryMethod = 's3_store' | 'email' | 'both';

export interface ReportSchedule {
  id: string;
  template_id: string;
  owner_id: string;
  name: string;
  cron_expression: string | null;
  interval_seconds: number | null;
  date_range_mode: DateRangeMode;
  enabled: boolean;
  delivery_method: DeliveryMethod;
  recipients: string[] | null;
  last_run_at: string | null;
  next_run_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface GeneratedReport {
  id: string;
  schedule_id: string | null;
  template_id: string | null;
  owner_id: string;
  title: string;
  file_size: number | null;
  date_range_mode: string | null;
  date_from: string | null;
  date_to: string | null;
  status: string;
  error_message: string | null;
  created_at: string;
}
