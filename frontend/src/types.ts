export type Severity = 'critical' | 'high' | 'medium' | 'low' | 'info';
export type FindingStatus = 'new' | 'reviewing' | 'confirmed' | 'dismissed' | 'resolved';
export type SourceType = 'tor_forum' | 'paste_site' | 'telegram' | 'breach_db' | 'ransomware_blog';
export type SourceHealth = 'healthy' | 'degraded' | 'offline';

export interface Client {
  id: number;
  name: string;
  created_at: string;
}

export interface Institution {
  id: number;
  client_id: number;
  client_name?: string;
  name: string;
  city: string;
  state: string;
  charter_number?: string;
  created_at: string;
}

export interface WatchTerm {
  id: number;
  institution_id: number;
  term_type: 'name' | 'domain' | 'bin' | 'routing_number';
  value: string;
  created_at: string;
}

export interface Finding {
  id: number;
  institution_id: number;
  institution_name?: string;
  source_type: SourceType;
  severity: Severity;
  status: FindingStatus;
  title: string;
  snippet: string;
  source_url?: string;
  discovered_at: string;
  updated_at: string;
}

export interface Source {
  id: number;
  name: string;
  source_type: SourceType;
  health: SourceHealth;
  last_poll: string;
  finding_count: number;
  avg_poll_seconds: number;
}

export interface DashboardStats {
  total_findings: number;
  findings_by_severity: Record<Severity, number>;
  new_today: number;
  monitored_institutions: number;
  active_sources: number;
  findings_trend: { date: string; count: number }[];
}
