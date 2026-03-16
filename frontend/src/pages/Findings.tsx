import { useEffect, useState, useCallback, useRef } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { fetchFindings, fetchInstitutions, updateFindingStatus } from '../api';
import { colors, card, font, statusLabel } from '../theme';
import SeverityBadge from '../components/SeverityBadge';
import StatusBadge from '../components/StatusBadge';
import type { Finding, Institution, Severity, FindingStatus } from '../types';
import { Search, ChevronDown, ChevronLeft, ChevronRight, ExternalLink, Calendar, Hash, User } from 'lucide-react';
import type { CSSProperties } from 'react';

const allSeverities: Severity[] = ['critical', 'high', 'medium', 'low', 'info'];
const allStatuses: FindingStatus[] = ['new', 'reviewing', 'escalated', 'false_positive', 'resolved'];
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
  const [sevFilter, setSevFilter] = useState(searchParams.get('severity') || '');
  const [statusFilter, setStatusFilter] = useState(searchParams.get('status') || '');
  const [instFilter, setInstFilter] = useState(searchParams.get('institution_id') || '');
  const [dateFilter, setDateFilter] = useState(searchParams.get('date') || '');
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
    if (sevFilter) params.severity = sevFilter;
    if (statusFilter) params.status = statusFilter;
    if (instFilter) params.institution_id = instFilter;
    if (dateFilter) {
      params.date_from = `${dateFilter}T00:00:00`;
      params.date_to = `${dateFilter}T23:59:59`;
    }
    if (debouncedSearch) params.q = debouncedSearch;
    fetchFindings(params as any).then(res => {
      setFindings(res.items);
      setTotal(res.total);
    });
  }, [sevFilter, statusFilter, instFilter, dateFilter, debouncedSearch, page]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { fetchInstitutions().then(setInstitutions); }, []);

  // Reset page when filters change
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
        <select style={selectStyle} value={sevFilter} onChange={e => handleFilterChange(setSevFilter)(e.target.value)}>
          <option value="">All Severities</option>
          {allSeverities.map(s => <option key={s} value={s}>{s.charAt(0).toUpperCase() + s.slice(1)}</option>)}
        </select>
        <select style={selectStyle} value={statusFilter} onChange={e => handleFilterChange(setStatusFilter)(e.target.value)}>
          <option value="">All Statuses</option>
          {allStatuses.map(s => <option key={s} value={s}>{statusLabel(s)}</option>)}
        </select>
        <select style={selectStyle} value={instFilter} onChange={e => handleFilterChange(setInstFilter)(e.target.value)}>
          <option value="">All Institutions</option>
          {institutions.map(i => <option key={i.id} value={i.id}>{i.name}</option>)}
        </select>
        <div style={{ position: 'relative' }}>
          <Calendar size={16} color={colors.textMuted} style={{ position: 'absolute', left: 12, top: 10 }} />
          <input
            type="date"
            style={{ ...selectStyle, paddingLeft: 36, minWidth: 160 }}
            value={dateFilter}
            onChange={e => handleFilterChange(setDateFilter)(e.target.value)}
          />
        </div>
        {(sevFilter || statusFilter || instFilter || search || dateFilter) && (
          <button
            onClick={() => { setSevFilter(''); setStatusFilter(''); setInstFilter(''); setSearch(''); setDateFilter(''); setPage(1); setSearchParams({}); }}
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
                      {f.source_url && (
                        <div style={{ marginTop: 10 }}>
                          <a
                            href={f.source_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            style={{ color: colors.accent, fontSize: 12, display: 'inline-flex', alignItems: 'center', gap: 4 }}
                          >
                            <ExternalLink size={12} /> Source link
                          </a>
                        </div>
                      )}
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

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}
