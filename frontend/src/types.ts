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
  appendix_full_content: boolean;
  fp_analytics: boolean;
  pattern_effectiveness: boolean;
  institution_threat_summary: boolean;
  analyst_performance: boolean;
}

export interface ReportChartOptions {
  severity_pie: boolean;
  status_pie: boolean;
  trend_line: boolean;
  source_bar: boolean;
  institution_bar: boolean;
  severity_trend: boolean;
  fp_rate_bar: boolean;
  disposition_pie: boolean;
  analyst_throughput_bar: boolean;
  threat_category_bar: boolean;
}

export interface ReportRequest {
  title: string;
  subtitle?: string;
  date_from?: string;
  date_to?: string;
  client_id?: string;
  institution_id?: string;
  severities?: string[];
  statuses?: string[];
  sections: ReportSections;
  charts: ReportChartOptions;
  truncate_content?: boolean;
}

export interface ReportTemplateConfig {
  title: string;
  subtitle?: string;
  client_id?: string;
  institution_id?: string;
  severities?: string[];
  statuses?: string[];
  sections: ReportSections;
  charts: ReportChartOptions;
  truncate_content?: boolean;
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

// BIN Database
export type CardBrand = 'visa' | 'mastercard' | 'amex' | 'discover' | 'jcb' | 'unionpay' | 'diners' | 'maestro' | 'other';
export type CardType = 'credit' | 'debit' | 'prepaid' | 'charge' | 'unknown';

export interface BINRecord {
  id: string;
  bin_prefix: string;
  bin_range_start: string | null;
  bin_range_end: string | null;
  issuer_name: string | null;
  card_brand: CardBrand | null;
  card_type: CardType | null;
  card_level: string | null;
  country_code: string | null;
  country_name: string | null;
  bank_url: string | null;
  bank_phone: string | null;
  source: string | null;
  updated_at: string | null;
}

export interface BINLookupResult {
  bin_prefix: string;
  found: boolean;
  issuer_name: string | null;
  card_brand: string | null;
  card_type: string | null;
  card_level: string | null;
  country_code: string | null;
  country_name: string | null;
  bank_url: string | null;
  bank_phone: string | null;
}

export interface BINStats {
  total_records: number;
  by_brand: Record<string, number>;
  by_source: Record<string, number>;
  by_country: { name: string; code: string; count: number }[];
  top_issuers: { name: string; count: number }[];
}

export interface BINImportResult {
  imported: number;
  updated: number;
  skipped: number;
  errors: string[];
  source: string;
}

// Institution Threat Summary
export interface ThreatCategoryBreakdown {
  category: string;
  count: number;
}

export interface SourceChannelBreakdown {
  source_type: string;
  count: number;
}

export interface ThreatSummary {
  institution_id: string;
  institution_name: string;
  findings_timeline: { date: string; count: number }[];
  threat_categories: ThreatCategoryBreakdown[];
  total_findings: number;
  confirmed_threats: number;
  active_threat_actors: number;
  top_source_channels: SourceChannelBreakdown[];
  by_severity: Record<string, number>;
  by_status: Record<string, number>;
  executive_brief: string;
}

// Alert Rules & Notifications

export interface AlertRule {
  id: string;
  name: string;
  owner_id: string;
  institution_id: string | null;
  min_severity: Severity;
  source_types: string[] | null;
  keyword_filter: string | null;
  enabled: boolean;
  notify_email: boolean;
  notify_slack: boolean;
  notify_webhook_url: string | null;
  created_at: string;
}

export interface AlertRuleCreate {
  name: string;
  owner_id: string;
  institution_id?: string | null;
  min_severity?: Severity;
  source_types?: string[] | null;
  keyword_filter?: string | null;
  enabled?: boolean;
  notify_email?: boolean;
  notify_slack?: boolean;
  notify_webhook_url?: string | null;
}

export interface AlertRuleUpdate {
  name?: string;
  institution_id?: string | null;
  min_severity?: Severity;
  source_types?: string[] | null;
  keyword_filter?: string | null;
  enabled?: boolean;
  notify_email?: boolean;
  notify_slack?: boolean;
  notify_webhook_url?: string | null;
}

export interface Notification {
  id: string;
  user_id: string;
  alert_rule_id: string | null;
  finding_id: string | null;
  title: string;
  message: string | null;
  read: boolean;
  created_at: string;
}

// Analytics / Disposition Dashboard

export interface InstitutionFPRate {
  institution_id: string;
  institution_name: string;
  total_findings: number;
  false_positives: number;
  dismissed: number;
  confirmed: number;
  fp_rate: number;
}

export interface PatternEffectiveness {
  total_mentions: number;
  total_promoted: number;
  total_suppressed: number;
  suppression_rate: number;
  fp_score_distribution: { bucket: string; count: number }[];
}

export interface AnalystWorkload {
  pending_review: number;
  avg_disposition_hours: number | null;
  disposition_breakdown: { status: string; count: number }[];
  by_analyst: { analyst: string; reviewed: number; pending: number }[];
}

export interface DispositionTrend {
  date: string;
  confirmed: number;
  dismissed: number;
  false_positive: number;
  escalated: number;
  new: number;
}

export interface DispositionAnalytics {
  institution_fp_rates: InstitutionFPRate[];
  pattern_effectiveness: PatternEffectiveness;
  analyst_workload: AnalystWorkload;
  disposition_trends: DispositionTrend[];
}
