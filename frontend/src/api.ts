import type { Client, Institution, WatchTerm, Finding, FindingDetail, Source, DashboardStats, FindingStatus, TelegramChannel, DiscordGuildChannel, PollTriggerResult, FindingTrend, RawMention, ExtractedFile, ExtractedFileSearchResult, DownloadQueueStatus } from './types';
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
}): Promise<Finding[]> {
  const qs = new URLSearchParams();
  if (params?.institution_id) qs.set('institution_id', params.institution_id);
  if (params?.severity) qs.set('severity', params.severity);
  if (params?.status) qs.set('status', params.status);
  if (params?.date_from) qs.set('date_from', params.date_from);
  if (params?.date_to) qs.set('date_to', params.date_to);
  const q = qs.toString();

  let fallback = [...mockFindings];
  if (params?.institution_id) fallback = fallback.filter(f => f.institution_id === params.institution_id);
  if (params?.severity) fallback = fallback.filter(f => f.severity === params.severity);
  if (params?.status) fallback = fallback.filter(f => f.status === params.status);
  if (params?.date_from) fallback = fallback.filter(f => f.discovered_at >= params.date_from!);
  if (params?.date_to) fallback = fallback.filter(f => f.discovered_at <= params.date_to!);

  return apiFetch(`/findings${q ? '?' + q : ''}`, fallback);
}

export async function fetchFinding(id: string): Promise<FindingDetail> {
  const fallback = mockFindingDetails.find(f => f.id === id) || mockFindingDetails[0];
  return apiFetch(`/findings/${id}`, fallback);
}

export async function updateFindingStatus(id: string, status: FindingStatus): Promise<Finding> {
  const fallback = { ...mockFindings.find(f => f.id === id)!, status };
  return apiFetch(`/findings/${id}/status`, fallback, {
    method: 'PUT',
    body: JSON.stringify({ status }),
  });
}

export async function fetchWatchTerms(institutionId: string): Promise<WatchTerm[]> {
  const fallback = mockWatchTerms.filter(w => w.institution_id === institutionId);
  return apiFetch(`/watch-terms?institution_id=${institutionId}`, fallback);
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

export async function fetchMentions(params?: {
  source_id?: string;
  source_type?: string;
  promoted?: boolean;
  q?: string;
}): Promise<RawMention[]> {
  const qs = new URLSearchParams();
  if (params?.source_id) qs.set('source_id', params.source_id);
  if (params?.source_type) qs.set('source_type', params.source_type);
  if (params?.promoted !== undefined) qs.set('promoted', String(params.promoted));
  if (params?.q) qs.set('q', params.q);
  const q = qs.toString();

  let fallback = [...mockRawMentions];
  if (params?.source_id) fallback = fallback.filter(m => m.source_id === params.source_id);
  if (params?.promoted !== undefined) {
    fallback = params.promoted
      ? fallback.filter(m => m.promoted_to_finding_id != null)
      : fallback.filter(m => !m.promoted_to_finding_id);
  }

  return apiFetch(`/mentions${q ? '?' + q : ''}`, fallback);
}

export async function fetchArchiveContents(
  type: 'mentions' | 'findings',
  id: string,
  q?: string,
): Promise<{ files: { filename: string; size: number; preview: string; content: string; extension?: string; sha256?: string; is_text?: boolean; s3_key?: string }[]; total: number }> {
  const qs = q ? `?q=${encodeURIComponent(q)}` : '';
  return apiFetch(`/${type}/${id}/archive-contents${qs}`, { files: [], total: 0 });
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

export async function fetchExtractedFilePreview(fileId: string): Promise<ExtractedFile> {
  return apiFetch(`/extracted-files/${fileId}/preview`, { filename: '', size: 0, content: '' } as ExtractedFile);
}

export async function searchExtractedFiles(params: {
  q: string;
  limit?: number;
  offset?: number;
}): Promise<ExtractedFileSearchResult> {
  const qs = new URLSearchParams({ q: params.q });
  if (params.limit) qs.set('limit', String(params.limit));
  if (params.offset) qs.set('offset', String(params.offset));
  return apiFetch(`/extracted-files/search?${qs}`, { query: params.q, total: 0, files: [] });
}

export async function fetchDownloadStatus(): Promise<DownloadQueueStatus> {
  return apiFetch('/pipeline/download-status', {
    current: null,
    pending: [],
    recent: [],
    stats: { total_pending: 0, total_stored: 0, total_errors: 0, total_extracted: 0 },
  });
}
