import type { Client, Institution, WatchTerm, Finding, FindingDetail, Source, DashboardStats, FindingStatus, TelegramChannel, DiscordGuildChannel, PollTriggerResult, FindingTrend } from './types';
import {
  mockClients, mockInstitutions, mockWatchTerms, mockFindings, mockFindingDetails,
  mockSources, mockDashboardStats,
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
