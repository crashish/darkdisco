import { useEffect, useState, useCallback } from 'react';
import { fetchFindings, fetchInstitutions, updateFindingStatus } from '../api';
import { colors, card, font } from '../theme';
import SeverityBadge from '../components/SeverityBadge';
import StatusBadge from '../components/StatusBadge';
import type { Finding, Institution, Severity, FindingStatus } from '../types';
import { Search, ChevronDown, ExternalLink } from 'lucide-react';
import type { CSSProperties } from 'react';

const allSeverities: Severity[] = ['critical', 'high', 'medium', 'low', 'info'];
const allStatuses: FindingStatus[] = ['new', 'reviewing', 'confirmed', 'dismissed', 'resolved'];

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

export default function Findings() {
  const [findings, setFindings] = useState<Finding[]>([]);
  const [institutions, setInstitutions] = useState<Institution[]>([]);
  const [search, setSearch] = useState('');
  const [sevFilter, setSevFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [instFilter, setInstFilter] = useState('');
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [statusMenuId, setStatusMenuId] = useState<number | null>(null);

  const load = useCallback(() => {
    const params: Record<string, string | number> = {};
    if (sevFilter) params.severity = sevFilter;
    if (statusFilter) params.status = statusFilter;
    if (instFilter) params.institution_id = Number(instFilter);
    fetchFindings(params as any).then(setFindings);
  }, [sevFilter, statusFilter, instFilter]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { fetchInstitutions().then(setInstitutions); }, []);

  const filtered = search
    ? findings.filter(f =>
        f.title.toLowerCase().includes(search.toLowerCase()) ||
        f.snippet.toLowerCase().includes(search.toLowerCase()) ||
        (f.institution_name || '').toLowerCase().includes(search.toLowerCase())
      )
    : findings;

  const handleStatusChange = async (id: number, status: FindingStatus) => {
    await updateFindingStatus(id, status);
    setStatusMenuId(null);
    setFindings(prev => prev.map(f => f.id === id ? { ...f, status } : f));
  };

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 700, marginBottom: 4 }}>Findings</h1>
      <p style={{ color: colors.textDim, fontSize: 14, marginBottom: 24 }}>
        {filtered.length} finding{filtered.length !== 1 ? 's' : ''} matching current filters
      </p>

      {/* Filters */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 20, flexWrap: 'wrap', alignItems: 'center' }}>
        <div style={{ position: 'relative' }}>
          <Search size={16} color={colors.textMuted} style={{ position: 'absolute', left: 12, top: 10 }} />
          <input
            style={inputStyle}
            placeholder="Search findings..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </div>
        <select style={selectStyle} value={sevFilter} onChange={e => setSevFilter(e.target.value)}>
          <option value="">All Severities</option>
          {allSeverities.map(s => <option key={s} value={s}>{s.charAt(0).toUpperCase() + s.slice(1)}</option>)}
        </select>
        <select style={selectStyle} value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
          <option value="">All Statuses</option>
          {allStatuses.map(s => <option key={s} value={s}>{s.charAt(0).toUpperCase() + s.slice(1)}</option>)}
        </select>
        <select style={selectStyle} value={instFilter} onChange={e => setInstFilter(e.target.value)}>
          <option value="">All Institutions</option>
          {institutions.map(i => <option key={i.id} value={i.id}>{i.name}</option>)}
        </select>
        {(sevFilter || statusFilter || instFilter || search) && (
          <button
            onClick={() => { setSevFilter(''); setStatusFilter(''); setInstFilter(''); setSearch(''); }}
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
            {filtered.map(f => (
              <>
                <tr
                  key={f.id}
                  style={{ borderBottom: `1px solid ${colors.border}`, cursor: 'pointer' }}
                  onClick={() => setExpandedId(expandedId === f.id ? null : f.id)}
                  onMouseEnter={e => (e.currentTarget.style.background = colors.bgHover)}
                  onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                >
                  <td style={{ padding: '10px 12px' }}><SeverityBadge severity={f.severity} /></td>
                  <td style={{ padding: '10px 12px', maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{f.title}</td>
                  <td style={{ padding: '10px 12px', color: colors.textDim }}>{f.institution_name}</td>
                  <td style={{ padding: '10px 12px', color: colors.textDim, fontFamily: font.mono, fontSize: 11 }}>{f.source_type}</td>
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
                            {s.charAt(0).toUpperCase() + s.slice(1)}
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
                        {f.snippet}
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
        {filtered.length === 0 && (
          <div style={{ textAlign: 'center', padding: 40, color: colors.textMuted }}>No findings match your filters.</div>
        )}
      </div>
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
