import { useEffect, useState, useCallback, useRef } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { fetchFindings, fetchInstitutions, updateFindingStatus } from '../api';
import { colors, card, font, statusLabel } from '../theme';
import SeverityBadge from '../components/SeverityBadge';
import StatusBadge from '../components/StatusBadge';
import type { Finding, Institution, Severity, FindingStatus } from '../types';
import { Search, ChevronDown, ChevronLeft, ChevronRight, ExternalLink, Calendar, Hash, User, MessageSquare } from 'lucide-react';
import type { CSSProperties } from 'react';
import MultiSelect from '../components/MultiSelect';

const allSeverities: Severity[] = ['critical', 'high', 'medium', 'low', 'info'];
const allStatuses: FindingStatus[] = ['new', 'reviewing', 'escalated', 'confirmed', 'dismissed', 'false_positive', 'resolved'];
const PAGE_SIZE = 50;

const selectStyle: CSSProperties = {
  background: colors.bgSurface,
  border: `1px solid ${colors.border}`,
  borderRadius: 6,
  color: colors.text,
  padding: '8px 12px',
  fontSize: 13,
  outline: 'none',
  cursor: 'pointer',
  appearance: 'none' as const,
  minWidth: 140,
};

const inputStyle: CSSProperties = {
  ...selectStyle,
  minWidth: 260,
  paddingLeft: 36,
};

const pageBtnStyle: CSSProperties = {
  background: colors.bgSurface,
  border: `1px solid ${colors.border}`,
  borderRadius: 6,
  color: colors.text,
  padding: '6px 10px',
  fontSize: 13,
  cursor: 'pointer',
  display: 'inline-flex',
  alignItems: 'center',
  gap: 4,
};

export default function Findings() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [findings, setFindings] = useState<Finding[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(Number(searchParams.get('page')) || 1);
  const [institutions, setInstitutions] = useState<Institution[]>([]);
  const [search, setSearch] = useState(searchParams.get('q') || '');
  const [debouncedSearch, setDebouncedSearch] = useState(search);
  const navigate = useNavigate();
  const [sevFilter, setSevFilter] = useState<Set<string>>(() => {
    const v = searchParams.get('severity');
    return v ? new Set(v.split(',').filter(Boolean)) : new Set();
  });
  const [statusFilter, setStatusFilter] = useState<Set<string>>(() => {
    const v = searchParams.get('status');
    return v ? new Set(v.split(',').filter(Boolean)) : new Set();
  });
  const [instFilter, setInstFilter] = useState<Set<string>>(() => {
    const v = searchParams.get('institution_id');
    return v ? new Set(v.split(',').filter(Boolean)) : new Set();
  });
  const [dateFrom, setDateFrom] = useState(searchParams.get('date_from') || '');
  const [dateTo, setDateTo] = useState(searchParams.get('date_to') || '');
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [statusMenuId, setStatusMenuId] = useState<string | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();

  // Debounce search input
  useEffect(() => {
    debounceRef.current = setTimeout(() => {
      setDebouncedSearch(search);
      setPage(1);
    }, 300);
    return () => clearTimeout(debounceRef.current);
  }, [search]);

  const load = useCallback(() => {
    const params: Record<string, string | number> = { page, page_size: PAGE_SIZE };
    if (sevFilter.size > 0) params.severity = Array.from(sevFilter).join(',');
    if (statusFilter.size > 0) params.status = Array.from(statusFilter).join(',');
    if (instFilter.size > 0) params.institution_id = Array.from(instFilter).join(',');
    if (dateFrom) params.date_from = dateFrom.includes('T') ? dateFrom : `${dateFrom}T00:00:00`;
    if (dateTo) params.date_to = dateTo.includes('T') ? dateTo : `${dateTo}T23:59:59`;
    if (debouncedSearch) params.q = debouncedSearch;
    fetchFindings(params as any).then(res => {
      setFindings(res.items);
      setTotal(res.total);
    });
  }, [sevFilter, statusFilter, instFilter, dateFrom, dateTo, debouncedSearch, page]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { fetchInstitutions().then(setInstitutions); }, []);

  // Reset page when set-based filters change
  const handleSetFilterChange = (setter: (v: Set<string>) => void) => (v: Set<string>) => {
    setter(v);
    setPage(1);
  };
  const handleFilterChange = (setter: (v: string) => void) => (v: string) => {
    setter(v);
    setPage(1);
  };

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  const handleStatusChange = async (id: string, status: FindingStatus) => {
    await updateFindingStatus(id, status);
    setStatusMenuId(null);
    setFindings(prev => prev.map(f => f.id === id ? { ...f, status } : f));
  };

  const channelName = (f: Finding): string | null => {
    const meta = f.metadata as Record<string, unknown> | null | undefined;
    return (meta?.channel_name as string) || (meta?.channel_ref as string) || (meta?.forum_name as string) || null;
  };

  const senderName = (f: Finding): string | null => {
    const meta = f.metadata as Record<string, unknown> | null | undefined;
    return (meta?.sender_name as string) || (meta?.post_author as string) || null;
  };

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 700, marginBottom: 4 }}>Findings</h1>
      <p style={{ color: colors.textDim, fontSize: 14, marginBottom: 24 }}>
        {total} finding{total !== 1 ? 's' : ''} matching current filters
      </p>

      {/* Filters */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 20, flexWrap: 'wrap', alignItems: 'center' }}>
        <div style={{ position: 'relative' }}>
          <Search size={16} color={colors.textMuted} style={{ position: 'absolute', left: 12, top: 10 }} />
          <input
            style={inputStyle}
            placeholder="Search title, summary, content..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </div>
        <MultiSelect
          label="Severity"
          options={allSeverities.map(s => ({ value: s, label: s.charAt(0).toUpperCase() + s.slice(1) }))}
          selected={sevFilter}
          onChange={handleSetFilterChange(setSevFilter)}
        />
        <MultiSelect
          label="Status"
          options={allStatuses.map(s => ({ value: s, label: statusLabel(s) }))}
          selected={statusFilter}
          onChange={handleSetFilterChange(setStatusFilter)}
        />
        <MultiSelect
          label="Institution"
          options={institutions.map(i => ({ value: i.id, label: i.name }))}
          selected={instFilter}
          onChange={handleSetFilterChange(setInstFilter)}
        />
        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
          <Calendar size={16} color={colors.textMuted} />
          <input
            type="date"
            style={{ ...selectStyle, minWidth: 140, padding: '7px 10px' }}
            value={dateFrom}
            onChange={e => { setDateFrom(e.target.value); setPage(1); }}
            title="From date"
          />
          <span style={{ color: colors.textMuted, fontSize: 12 }}>&ndash;</span>
          <input
            type="date"
            style={{ ...selectStyle, minWidth: 140, padding: '7px 10px' }}
            value={dateTo}
            onChange={e => { setDateTo(e.target.value); setPage(1); }}
            title="To date"
          />
          <input
            type="text"
            placeholder="e.g. 30d, 1w, 3m"
            style={{ ...selectStyle, minWidth: 110, padding: '7px 10px', fontSize: 12 }}
            onKeyDown={e => {
              if (e.key === 'Enter') {
                const range = parseDateShorthand((e.target as HTMLInputElement).value);
                if (range) { setDateFrom(range.from); setDateTo(range.to); setPage(1); (e.target as HTMLInputElement).value = ''; }
              }
            }}
            title="Type shorthand (24h, 7d, 1w, 3m) and press Enter"
          />
        </div>
        <div style={{ display: 'flex', gap: 4 }}>
          {[
            { label: '24h', value: '24h' },
            { label: '7d', value: '7d' },
            { label: '30d', value: '30d' },
            { label: '90d', value: '90d' },
          ].map(btn => (
            <button
              key={btn.value}
              onClick={() => { const range = parseDateShorthand(btn.value); if (range) { setDateFrom(range.from); setDateTo(range.to); setPage(1); } }}
              style={{
                background: 'none', border: `1px solid ${colors.border}`, borderRadius: 4,
                color: colors.accent, fontSize: 11, padding: '4px 8px', cursor: 'pointer',
              }}
            >
              {btn.label}
            </button>
          ))}
        </div>
        {(sevFilter.size > 0 || statusFilter.size > 0 || instFilter.size > 0 || search || dateFrom || dateTo) && (
          <button
            onClick={() => { setSevFilter(new Set()); setStatusFilter(new Set()); setInstFilter(new Set()); setSearch(''); setDateFrom(''); setDateTo(''); setPage(1); setSearchParams({}); }}
            style={{ background: 'none', border: 'none', color: colors.accent, fontSize: 13, cursor: 'pointer', padding: '8px 4px' }}
          >
            Clear filters
          </button>
        )}
      </div>

      {/* Table */}
      <div style={card}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
              {['Severity', 'Title', 'Institution', 'Source', 'Status', 'Discovered'].map(h => (
                <th key={h} style={{ textAlign: 'left', padding: '8px 12px', color: colors.textMuted, fontWeight: 500, fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.05em' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {findings.map(f => (
              <>
                <tr
                  key={f.id}
                  style={{ borderBottom: `1px solid ${colors.border}`, cursor: 'pointer' }}
                  onClick={() => setExpandedId(expandedId === f.id ? null : f.id)}
                  onMouseEnter={e => (e.currentTarget.style.background = colors.bgHover)}
                  onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                >
                  <td style={{ padding: '10px 12px' }}><SeverityBadge severity={f.severity} /></td>
                  <td
                    style={{ padding: '10px 12px', maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', cursor: 'pointer' }}
                    onClick={e => { e.stopPropagation(); navigate(`/findings/${f.id}`); }}
                    onMouseEnter={e => (e.currentTarget.style.color = colors.accent)}
                    onMouseLeave={e => (e.currentTarget.style.color = colors.text)}
                  >{f.title}</td>
                  <td style={{ padding: '10px 12px', color: colors.textDim }}>{f.institution_name}</td>
                  <td style={{ padding: '10px 12px', color: colors.textDim }}>
                    <span style={{ fontFamily: font.mono, fontSize: 11 }}>{f.source_type}</span>
                    {channelName(f) && (
                      <span style={{ display: 'flex', alignItems: 'center', gap: 3, fontSize: 11, color: colors.textMuted, marginTop: 2 }}>
                        <Hash size={10} /> {channelName(f)}
                      </span>
                    )}
                    {senderName(f) && (
                      <span style={{ display: 'flex', alignItems: 'center', gap: 3, fontSize: 11, color: colors.textMuted, marginTop: 1 }}>
                        <User size={10} /> {senderName(f)}
                      </span>
                    )}
                  </td>
                  <td style={{ padding: '10px 12px', position: 'relative' }}>
                    <div
                      style={{ display: 'inline-flex', alignItems: 'center', gap: 4, cursor: 'pointer' }}
                      onClick={e => { e.stopPropagation(); setStatusMenuId(statusMenuId === f.id ? null : f.id); }}
                    >
                      <StatusBadge status={f.status} />
                      <ChevronDown size={12} color={colors.textMuted} />
                    </div>
                    {statusMenuId === f.id && (
                      <div style={{
                        position: 'absolute', top: '100%', left: 12, zIndex: 50,
                        background: colors.bgSurface, border: `1px solid ${colors.border}`, borderRadius: 6,
                        padding: 4, minWidth: 140, boxShadow: '0 8px 24px rgba(0,0,0,0.4)',
                      }}>
                        {allStatuses.filter(s => s !== f.status).map(s => (
                          <div
                            key={s}
                            style={{ padding: '6px 10px', fontSize: 12, cursor: 'pointer', borderRadius: 4, color: colors.textDim }}
                            onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = colors.bgHover; (e.currentTarget as HTMLElement).style.color = colors.text; }}
                            onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = 'transparent'; (e.currentTarget as HTMLElement).style.color = colors.textDim; }}
                            onClick={e => { e.stopPropagation(); handleStatusChange(f.id, s); }}
                          >
                            {statusLabel(s)}
                          </div>
                        ))}
                      </div>
                    )}
                  </td>
                  <td style={{ padding: '10px 12px', color: colors.textMuted, fontSize: 12 }}>{timeAgo(f.discovered_at)}</td>
                </tr>
                {expandedId === f.id && (
                  <tr key={`${f.id}-detail`} style={{ background: colors.bgSurface }}>
                    <td colSpan={6} style={{ padding: '16px 24px' }}>
                      <div style={{ fontSize: 13, lineHeight: 1.6, color: colors.textDim, maxWidth: 800 }}>
                        {f.summary}
                      </div>
                      <div style={{ marginTop: 10, display: 'flex', gap: 12, alignItems: 'center' }}>
                        {f.mention_id && (
                          <button
                            onClick={e => { e.stopPropagation(); navigate(`/mentions?mention=${f.mention_id}`); }}
                            style={{
                              display: 'inline-flex', alignItems: 'center', gap: 4,
                              padding: '3px 8px', fontSize: 11, fontWeight: 600,
                              background: 'none', border: `1px solid ${colors.border}`,
                              borderRadius: 4, color: colors.accent, cursor: 'pointer',
                            }}
                            title="View the source message that generated this finding"
                          >
                            <MessageSquare size={11} /> View source message
                          </button>
                        )}
                        {f.source_url && (
                          <a
                            href={f.source_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            style={{ display: 'inline-flex', alignItems: 'center', gap: 4, padding: '3px 8px', fontSize: 11, background: 'none', border: `1px solid ${colors.border}`, borderRadius: 4, color: colors.textDim, textDecoration: 'none' }}
                          >
                            <ExternalLink size={12} /> Original source
                          </a>
                        )}
                      </div>
                      <div style={{ marginTop: 8, fontSize: 11, color: colors.textMuted }}>
                        ID: {f.id} &middot; Updated: {new Date(f.updated_at).toLocaleString()}
                      </div>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
        {findings.length === 0 && (
          <div style={{ textAlign: 'center', padding: 40, color: colors.textMuted }}>No findings match your filters.</div>
        )}
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 16 }}>
          <span style={{ fontSize: 12, color: colors.textMuted }}>
            Showing {(page - 1) * PAGE_SIZE + 1}&ndash;{Math.min(page * PAGE_SIZE, total)} of {total}
          </span>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <button
              style={{ ...pageBtnStyle, opacity: page <= 1 ? 0.4 : 1 }}
              disabled={page <= 1}
              onClick={() => setPage(p => Math.max(1, p - 1))}
            >
              <ChevronLeft size={14} /> Prev
            </button>
            <span style={{ fontSize: 13, color: colors.textDim }}>
              Page {page} of {totalPages}
            </span>
            <button
              style={{ ...pageBtnStyle, opacity: page >= totalPages ? 0.4 : 1 }}
              disabled={page >= totalPages}
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
            >
              Next <ChevronRight size={14} />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function parseDateShorthand(input: string): { from: string; to: string } | null {
  const trimmed = input.trim().toLowerCase();
  const match = trimmed.match(/^(\d+)\s*(h|d|w|m)$/);
  if (!match) return null;
  const amount = parseInt(match[1], 10);
  const unit = match[2];
  const now = new Date();
  const from = new Date(now);
  switch (unit) {
    case 'h': from.setHours(from.getHours() - amount); break;
    case 'd': from.setDate(from.getDate() - amount); break;
    case 'w': from.setDate(from.getDate() - amount * 7); break;
    case 'm': from.setMonth(from.getMonth() - amount); break;
    default: return null;
  }
  const fmt = (d: Date) => d.toISOString().slice(0, 10);
  return { from: fmt(from), to: fmt(now) };
}

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}
