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

export interface Source {
  id: string;
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
