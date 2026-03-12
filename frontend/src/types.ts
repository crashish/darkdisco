export type Severity = 'critical' | 'high' | 'medium' | 'low' | 'info';
export type FindingStatus = 'new' | 'reviewing' | 'confirmed' | 'dismissed' | 'resolved';
export type SourceType = 'tor_forum' | 'paste_site' | 'telegram' | 'breach_db' | 'ransomware_blog';
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
}

export interface FindingDetail extends Finding {
  raw_content: string | null;
  matched_terms: MatchedTerm[] | null;
  tags: string[] | null;
  analyst_notes: string | null;
  enrichment: EnrichmentData | null;
  status_history: StatusHistoryEntry[];
  created_at: string;
  reviewed_by: string | null;
  reviewed_at: string | null;
}

export interface MatchedTerm {
  term_id: number;
  term_type: string;
  value: string;
  context?: string;
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

export interface Source {
  id: string;
  name: string;
  source_type: SourceType;
  health: SourceHealth;
  last_poll: string;
  finding_count: number;
  avg_poll_seconds: number;
}

export interface SourceDetail {
  id: string;
  name: string;
  source_type: SourceType;
  url: string | null;
  connector_class: string | null;
  enabled: boolean;
  poll_interval_seconds: number;
  last_polled_at: string | null;
  last_error: string | null;
  config: Record<string, unknown> | null;
  created_at: string;
  health: SourceHealth;
  finding_count: number;
  poll_history: PollHistoryEntry[];
  findings_by_day: { date: string; count: number }[];
}

export interface PollHistoryEntry {
  polled_at: string;
  duration_ms: number;
  findings_found: number;
  status: 'success' | 'error' | 'partial';
  error?: string;
}

export interface DashboardStats {
  total_findings: number;
  findings_by_severity: Record<Severity, number>;
  new_today: number;
  monitored_institutions: number;
  active_sources: number;
  findings_trend: { date: string; count: number }[];
}
