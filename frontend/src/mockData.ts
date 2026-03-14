import type {
  Client, Institution, WatchTerm, Finding, FindingDetail, Source, DashboardStats,
  Severity, FindingStatus, SourceType, MatchedTerm, StatusHistoryEntry, EnrichmentData,
  RawMention,
} from './types';

const now = new Date();
const daysAgo = (n: number) => new Date(now.getTime() - n * 86400000).toISOString();
const hoursAgo = (n: number) => new Date(now.getTime() - n * 3600000).toISOString();
const minsAgo = (n: number) => new Date(now.getTime() - n * 60000).toISOString();

export const mockClients: Client[] = [
  { id: 'c-001', name: 'Heartland Bancshares', created_at: daysAgo(120) },
  { id: 'c-002', name: 'Prairie Financial Group', created_at: daysAgo(90) },
  { id: 'c-003', name: 'Coastal CU Holdings', created_at: daysAgo(60) },
];

export const mockInstitutions: Institution[] = [
  { id: 'i-001', client_id: 'c-001', client_name: 'Heartland Bancshares', name: 'First Community Bank', city: 'Springfield', state: 'IL', charter_number: 'CB-44012', created_at: daysAgo(120) },
  { id: 'i-002', client_id: 'c-001', client_name: 'Heartland Bancshares', name: 'Heartland Savings & Loan', city: 'Decatur', state: 'IL', charter_number: 'SL-22198', created_at: daysAgo(115) },
  { id: 'i-003', client_id: 'c-002', client_name: 'Prairie Financial Group', name: 'Prairie State Credit Union', city: 'Des Moines', state: 'IA', charter_number: 'CU-88341', created_at: daysAgo(90) },
  { id: 'i-004', client_id: 'c-002', client_name: 'Prairie Financial Group', name: 'Midwest Heritage Bank', city: 'Omaha', state: 'NE', charter_number: 'CB-55203', created_at: daysAgo(85) },
  { id: 'i-005', client_id: 'c-003', client_name: 'Coastal CU Holdings', name: 'Harbor Federal Credit Union', city: 'Charleston', state: 'SC', charter_number: 'CU-71009', created_at: daysAgo(60) },
  { id: 'i-006', client_id: 'c-003', client_name: 'Coastal CU Holdings', name: 'Tideline Community CU', city: 'Savannah', state: 'GA', charter_number: 'CU-71055', created_at: daysAgo(55) },
];

export const mockWatchTerms: WatchTerm[] = [
  { id: 'w-001', institution_id: 'i-001', term_type: 'name', value: 'First Community Bank', created_at: daysAgo(120) },
  { id: 'w-002', institution_id: 'i-001', term_type: 'domain', value: 'firstcommunitybank.com', created_at: daysAgo(120) },
  { id: 'w-003', institution_id: 'i-001', term_type: 'bin', value: '414720', created_at: daysAgo(110) },
  { id: 'w-004', institution_id: 'i-001', term_type: 'routing_number', value: '071902843', created_at: daysAgo(120) },
  { id: 'w-005', institution_id: 'i-002', term_type: 'name', value: 'Heartland Savings', created_at: daysAgo(115) },
  { id: 'w-006', institution_id: 'i-002', term_type: 'domain', value: 'heartlandsavings.com', created_at: daysAgo(115) },
  { id: 'w-007', institution_id: 'i-003', term_type: 'name', value: 'Prairie State Credit Union', created_at: daysAgo(90) },
  { id: 'w-008', institution_id: 'i-003', term_type: 'domain', value: 'prairiestatescu.org', created_at: daysAgo(90) },
  { id: 'w-009', institution_id: 'i-003', term_type: 'bin', value: '523841', created_at: daysAgo(85) },
  { id: 'w-010', institution_id: 'i-004', term_type: 'name', value: 'Midwest Heritage Bank', created_at: daysAgo(85) },
  { id: 'w-011', institution_id: 'i-004', term_type: 'routing_number', value: '104000016', created_at: daysAgo(85) },
  { id: 'w-012', institution_id: 'i-005', term_type: 'name', value: 'Harbor Federal Credit Union', created_at: daysAgo(60) },
  { id: 'w-013', institution_id: 'i-005', term_type: 'domain', value: 'harborfcu.org', created_at: daysAgo(60) },
  { id: 'w-014', institution_id: 'i-006', term_type: 'name', value: 'Tideline Community', created_at: daysAgo(55) },
  { id: 'w-015', institution_id: 'i-006', term_type: 'domain', value: 'tidelinecu.org', created_at: daysAgo(55) },
];

const severities: Severity[] = ['critical', 'high', 'medium', 'low', 'info'];
const statuses: FindingStatus[] = ['new', 'reviewing', 'escalated', 'false_positive', 'resolved'];
const sourceTypes: SourceType[] = ['tor_forum', 'paste_site', 'telegram', 'breach_db', 'ransomware_blog'];

const findingTemplates: { title: string; summary: string; severity: Severity; source_type: SourceType }[] = [
  { title: 'Employee credential dump on dark forum', summary: 'Found 23 email:password pairs matching @firstcommunitybank.com in a paste uploaded to DarkLeaks forum. Credentials appear to be from a third-party breach. Recommend immediate password reset.', severity: 'critical', source_type: 'tor_forum' },
  { title: 'BIN 414720 offered in carding marketplace', summary: 'Threat actor "cr3d1tgh0st" listing 150+ cards with BIN 414720 at $15/card on DarkMarket. Batch appears fresh (< 7 days). Geographic cluster suggests POS compromise in IL region.', severity: 'critical', source_type: 'tor_forum' },
  { title: 'Phishing kit targeting institution login page', summary: 'Discovered phishing kit on paste site replicating firstcommunitybank.com/login. Kit includes SMS OTP bypass module. Hosted at 185.xx.xx.xx with Cloudflare fronting.', severity: 'high', source_type: 'paste_site' },
  { title: 'Routing number mentioned in ACH fraud discussion', summary: 'Routing number 071902843 referenced in Telegram channel focused on ACH/wire fraud. Discussion around testing small-amount transfers. 14 channel members engaged.', severity: 'high', source_type: 'telegram' },
  { title: 'Institution name in ransomware victim list', summary: 'Midwest Heritage Bank appeared on LockBit 3.0 affiliate blog as potential target. No data leak yet - appears to be in reconnaissance/targeting phase. Listed alongside 8 other regional banks.', severity: 'critical', source_type: 'ransomware_blog' },
  { title: 'Domain typosquat registration detected', summary: 'New domain harborfcu-secure.org registered via Namecheap with privacy protection. SSL cert issued by Let\'s Encrypt. Domain resolves to known bulletproof hosting provider.', severity: 'high', source_type: 'paste_site' },
  { title: 'Customer PII in breach database', summary: 'Breach database "FinLeaks_2024" contains 340 records with Prairie State Credit Union member data including names, SSN fragments, and account types. Data appears 6-8 months old.', severity: 'critical', source_type: 'breach_db' },
  { title: 'ATM skimmer discussion mentioning region', summary: 'Forum thread discussing ATM skimmer placement opportunities in Des Moines/Omaha corridor. Mentions "credit union ATMs with older NCR hardware" as preferred targets.', severity: 'medium', source_type: 'tor_forum' },
  { title: 'Mobile banking app reverse engineering', summary: 'Threat actor sharing decompiled APK analysis of a white-label mobile banking app used by several community banks. Identified hardcoded API endpoints and weak cert pinning.', severity: 'medium', source_type: 'telegram' },
  { title: 'Social engineering playbook for CU employees', summary: 'Detailed social engineering script targeting credit union call centers. Includes pretext scenarios for account takeover, wire transfer authorization, and password reset procedures.', severity: 'medium', source_type: 'tor_forum' },
  { title: 'Bulk email list with institution domains', summary: 'Marketing-style email list containing 89 addresses @heartlandsavings.com found in spam/phishing operator data dump. Mix of employee and generic department addresses.', severity: 'low', source_type: 'breach_db' },
  { title: 'Mention in general banking threat discussion', summary: 'Tideline Community CU mentioned in passing during a general discussion about southeastern US community banks. No specific targeting indicators. Monitoring for escalation.', severity: 'info', source_type: 'telegram' },
  { title: 'Credential stuffing target list', summary: 'Online banking portals for 45 community banks including prairiestatescu.org found in credential stuffing tool configuration file shared on dark web forum.', severity: 'high', source_type: 'tor_forum' },
  { title: 'Wire transfer fraud ring targeting small banks', summary: 'FBI flash alert cross-referenced: organized group targeting banks with assets < $1B in midwest region. TTPs match activity observed against Heartland Savings & Loan.', severity: 'high', source_type: 'telegram' },
  { title: 'Expired SSL certificate monitoring', summary: 'Domain tidelinecu.org SSL certificate approaching expiration flagged in automated scan. Not a direct threat but creates phishing opportunity if not renewed.', severity: 'info', source_type: 'paste_site' },
];

const instIds = ['i-001', 'i-002', 'i-003', 'i-004', 'i-005', 'i-006'];

export const mockFindings: Finding[] = findingTemplates.map((t, i) => ({
  id: `f-${String(i + 1).padStart(3, '0')}`,
  institution_id: instIds[i % 6],
  institution_name: mockInstitutions[i % 6].name,
  source_type: t.source_type,
  severity: t.severity,
  status: i < 4 ? 'new' : i < 7 ? 'reviewing' : i < 10 ? 'escalated' : i < 13 ? 'resolved' : 'false_positive' as FindingStatus,
  title: t.title,
  summary: t.summary,
  source_url: i % 3 === 0 ? undefined : `https://darkweb.example/${i}`,
  discovered_at: hoursAgo(i * 3 + Math.random() * 10),
  updated_at: hoursAgo(i * 2),
}));

export const mockSources: Source[] = [
  { id: 's-001', name: 'DarkLeaks Forum Monitor', source_type: 'tor_forum', health: 'healthy', enabled: true, last_poll: minsAgo(3), finding_count: 847, avg_poll_seconds: 300, poll_interval_seconds: 300, last_polled_at: minsAgo(3), last_error: null },
  { id: 's-002', name: 'PasteBin/GhostBin Scraper', source_type: 'paste_site', health: 'healthy', enabled: true, last_poll: minsAgo(1), finding_count: 1243, avg_poll_seconds: 60, poll_interval_seconds: 60, last_polled_at: minsAgo(1), last_error: null },
  { id: 's-003', name: 'Telegram Channel Ingest', source_type: 'telegram', health: 'degraded', enabled: true, last_poll: minsAgo(45), finding_count: 562, avg_poll_seconds: 120, poll_interval_seconds: 120, last_polled_at: minsAgo(45), last_error: 'Connection timeout: Telegram API rate limited after 50 requests' },
  { id: 's-004', name: 'Breach Database Correlator', source_type: 'breach_db', health: 'healthy', enabled: true, last_poll: minsAgo(12), finding_count: 2105, avg_poll_seconds: 600, poll_interval_seconds: 600, last_polled_at: minsAgo(12), last_error: null },
  { id: 's-005', name: 'Ransomware Blog Tracker', source_type: 'ransomware_blog', health: 'healthy', enabled: true, last_poll: minsAgo(8), finding_count: 189, avg_poll_seconds: 900, poll_interval_seconds: 900, last_polled_at: minsAgo(8), last_error: null },
  { id: 's-006', name: 'DarkMarket Carding Monitor', source_type: 'tor_forum', health: 'offline', enabled: false, last_poll: hoursAgo(6), finding_count: 331, avg_poll_seconds: 300, poll_interval_seconds: 300, last_polled_at: hoursAgo(6), last_error: null },
];

export const mockDashboardStats: DashboardStats = {
  total_findings: mockFindings.length,
  findings_by_severity: {
    critical: mockFindings.filter(f => f.severity === 'critical').length,
    high: mockFindings.filter(f => f.severity === 'high').length,
    medium: mockFindings.filter(f => f.severity === 'medium').length,
    low: mockFindings.filter(f => f.severity === 'low').length,
    info: mockFindings.filter(f => f.severity === 'info').length,
  },
  new_today: mockFindings.filter(f => f.status === 'new').length,
  monitored_institutions: mockInstitutions.length,
  active_sources: mockSources.filter(s => s.health !== 'offline').length,
  findings_trend: Array.from({ length: 14 }, (_, i) => ({
    date: daysAgo(13 - i).split('T')[0],
    count: Math.floor(Math.random() * 8) + 1,
  })),
};

function buildStatusHistory(status: FindingStatus, discoveredAt: string): StatusHistoryEntry[] {
  const transitions: FindingStatus[] = ['new', 'reviewing', 'escalated', 'false_positive', 'resolved'];
  const idx = transitions.indexOf(status);
  const history: StatusHistoryEntry[] = [
    { status: 'new', changed_at: discoveredAt, changed_by: 'system', notes: 'Auto-created by pipeline' },
  ];
  if (idx >= 1) history.push({ status: 'reviewing', changed_at: hoursAgo(Math.max(0, idx * 4 - 2)), changed_by: 'analyst@example.com', notes: 'Assigned for triage' });
  if (idx >= 2 && status !== 'false_positive') history.push({ status: 'escalated', changed_at: hoursAgo(Math.max(0, idx * 2 - 1)), changed_by: 'analyst@example.com', notes: 'Verified as legitimate threat' });
  if (status === 'false_positive') history.push({ status: 'false_positive', changed_at: hoursAgo(1), changed_by: 'analyst@example.com', notes: 'False positive - generic mention' });
  if (status === 'resolved') history.push({ status: 'resolved', changed_at: hoursAgo(1), changed_by: 'analyst@example.com', notes: 'Mitigated - credentials rotated' });
  return history;
}

function buildMatchedTerms(institutionId: string): MatchedTerm[] {
  const terms = mockWatchTerms.filter(w => w.institution_id === institutionId);
  return terms.slice(0, 3).map(t => ({
    term_id: Number(t.id.replace(/\D/g, '')),
    term_type: t.term_type,
    value: t.value,
    context: `...matched "${t.value}" in source content...`,
  }));
}

function buildEnrichment(severity: string, i: number): EnrichmentData {
  return {
    dedup: { similarity_score: 0.15 + (i % 5) * 0.12, action: 'create' },
    false_positive: {
      is_fp: false,
      confidence: 0.85 + (i % 3) * 0.05,
      reason: severity === 'info' ? 'Low-confidence generic mention' : undefined,
    },
    threat_intel: i % 3 === 0 ? { ioc_matches: 2, threat_actors: ['cr3d1tgh0st'], campaigns: ['FIN-2024-Q4'] } : undefined,
  };
}

function buildMetadata(sourceType: SourceType, i: number): Record<string, unknown> | null {
  if (sourceType === 'telegram') {
    return {
      channel_name: i % 2 === 0 ? 'DarkMarket Signals' : 'FinFraud Discussion',
      channel_id: -100100 + i,
      message_id: 14200 + i * 37,
      message_date: hoursAgo(i * 3 + 1),
      sender_name: i % 3 === 0 ? 'cr3d1tgh0st' : i % 3 === 1 ? 'darkvendor_42' : null,
      forwarded_from: i % 4 === 0 ? { channel: 'Underground Leaks', message_id: 8800 + i } : null,
      has_media: i % 5 === 0,
      media_type: i % 5 === 0 ? 'document' : null,
      media_filename: i % 5 === 0 ? 'dump_2024.csv' : null,
      reply_to_message_id: i % 6 === 0 ? 14100 + i : null,
    };
  }
  if (sourceType === 'tor_forum') {
    return {
      forum_name: 'DarkLeaks Forum',
      thread_title: 'Fresh dumps - regional banks',
      post_author: 'threatactor_' + (i + 1),
      post_date: hoursAgo(i * 3 + 2),
      thread_url: `http://darkleak.onion/thread/${1000 + i}`,
    };
  }
  return null;
}

export const mockFindingDetails: FindingDetail[] = mockFindings.map((f, i) => ({
  ...f,
  raw_content: findingTemplates[i].summary + '\n\n[Raw scraped content from source. May contain formatting artifacts, markup, and surrounding context from the original post.]',
  matched_terms: buildMatchedTerms(f.institution_id),
  tags: i % 4 === 0 ? ['credential-leak', 'priority'] : i % 3 === 0 ? ['carding', 'financial-fraud'] : i % 2 === 0 ? ['phishing'] : null,
  analyst_notes: i < 8 ? `Initial triage complete. ${i % 2 === 0 ? 'Escalated to incident response team.' : 'Monitoring for further activity.'}` : null,
  enrichment: buildEnrichment(f.severity, i),
  status_history: buildStatusHistory(f.status, f.discovered_at),
  created_at: f.discovered_at,
  reviewed_by: i < 10 ? 'analyst@example.com' : null,
  reviewed_at: i < 10 ? hoursAgo(i + 1) : null,
  source_name: mockSources[i % mockSources.length].name,
  metadata: buildMetadata(f.source_type, i),
}));

const mentionContents = [
  'Selling fresh CC batch from midwest region. 200+ cards. BINs include 4147xx, 5238xx. PM for bulk pricing. Wickr: ghostcc',
  'New database leak: FinanceCorpHQ 2024. Contains employee records, internal memos, and customer SSN fragments. 2.3GB total. Available on our private tracker.',
  'Looking for someone experienced in wire transfer procedures at community banks. Need help with ACH verification process. Serious inquiries only.',
  'Phishing kit v4.2 updated - now supports 2FA bypass for online banking portals. Works on most white-label platforms. Includes SMS intercept module.',
  'Anyone have intel on credit unions in the southeast? Looking for ones with older security infrastructure. Willing to trade info.',
  'Leaked internal IT audit report from a regional bank. Shows unpatched systems, default credentials on network devices. Very juicy.',
  'New stealer log dump - 45K entries from US banking customers. Includes saved passwords, cookies, and autofill data. Sorted by state.',
  'Ransomware negotiation chat log leaked. Shows bank paid $2.3M in BTC. Contains internal communications and incident response timeline.',
  'Telegram bot selling real-time BIN lookups. $5/query or $200/month unlimited. Claims data is sourced from POS terminals.',
  'Discussion about ATM jackpotting techniques for NCR and Diebold Nixdorf machines. Includes firmware modification guides.',
  'Credential pair list for online banking: 1,200 email:password combos. Mix of @gmail, @yahoo, @outlook. Claimed >40% hit rate.',
  'Dark web marketplace listing: cloned debit cards with PINs. Ships worldwide. Escrow available. 95% positive feedback.',
];

export const mockRawMentions: RawMention[] = mentionContents.map((content, i) => ({
  id: `m-${String(i + 1).padStart(3, '0')}`,
  source_id: mockSources[i % mockSources.length].id,
  source_name: mockSources[i % mockSources.length].name,
  source_type: mockSources[i % mockSources.length].source_type,
  content,
  content_hash: `sha256_${i.toString(16).padStart(8, '0')}`,
  source_url: i % 3 === 0 ? undefined : `https://darkweb.example/raw/${i}`,
  metadata: i % mockSources.length === 2
    ? { channel_name: 'Underground Markets', message_id: 9900 + i, message_date: hoursAgo(i * 2 + 1), sender_name: i % 2 === 0 ? 'anonvendor' : null }
    : i % mockSources.length === 0
    ? { forum_name: 'DarkLeaks Forum', thread_title: 'General trading', post_author: 'user_' + i }
    : undefined,
  collected_at: hoursAgo(i * 2 + Math.random() * 5),
  promoted_to_finding_id: i < 2 ? `f-${String(i + 1).padStart(3, '0')}` : null,
}));
