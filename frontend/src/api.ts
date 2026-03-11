import type { Client, Institution, WatchTerm, Finding, Source, DashboardStats, FindingStatus } from './types';
import {
  mockClients, mockInstitutions, mockWatchTerms, mockFindings,
  mockSources, mockDashboardStats,
} from './mockData';

const BASE = '/api';

async function apiFetch<T>(url: string, fallback: T, init?: RequestInit): Promise<T> {
  try {
    const res = await fetch(`${BASE}${url}`, {
      headers: { 'Content-Type': 'application/json' },
      ...init,
    });
    if (!res.ok) throw new Error(`${res.status}`);
    return await res.json() as T;
  } catch {
    console.warn(`API unavailable for ${url}, using mock data`);
    return fallback;
  }
}

export async function fetchDashboardStats(): Promise<DashboardStats> {
  return apiFetch('/dashboard/stats', mockDashboardStats);
}

export async function fetchClients(): Promise<Client[]> {
  return apiFetch('/clients', mockClients);
}

export async function fetchInstitutions(clientId?: number): Promise<Institution[]> {
  const qs = clientId ? `?client_id=${clientId}` : '';
  const fallback = clientId
    ? mockInstitutions.filter(i => i.client_id === clientId)
    : mockInstitutions;
  return apiFetch(`/institutions${qs}`, fallback);
}

export async function fetchFindings(params?: {
  institution_id?: number;
  severity?: string;
  status?: string;
}): Promise<Finding[]> {
  const qs = new URLSearchParams();
  if (params?.institution_id) qs.set('institution_id', String(params.institution_id));
  if (params?.severity) qs.set('severity', params.severity);
  if (params?.status) qs.set('status', params.status);
  const q = qs.toString();

  let fallback = [...mockFindings];
  if (params?.institution_id) fallback = fallback.filter(f => f.institution_id === params.institution_id);
  if (params?.severity) fallback = fallback.filter(f => f.severity === params.severity);
  if (params?.status) fallback = fallback.filter(f => f.status === params.status);

  return apiFetch(`/findings${q ? '?' + q : ''}`, fallback);
}

export async function updateFindingStatus(id: number, status: FindingStatus): Promise<Finding> {
  const fallback = { ...mockFindings.find(f => f.id === id)!, status };
  return apiFetch(`/findings/${id}/status`, fallback, {
    method: 'PUT',
    body: JSON.stringify({ status }),
  });
}

export async function fetchWatchTerms(institutionId: number): Promise<WatchTerm[]> {
  const fallback = mockWatchTerms.filter(w => w.institution_id === institutionId);
  return apiFetch(`/watch-terms?institution_id=${institutionId}`, fallback);
}

export async function fetchSources(): Promise<Source[]> {
  return apiFetch('/sources', mockSources);
}
