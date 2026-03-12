import { useEffect, useState } from 'react';
import { fetchClients, fetchInstitutions, fetchFindings, fetchWatchTerms } from '../api';
import { colors, card, font, severityColor, severityBg, badge as makeBadge } from '../theme';
import SeverityBadge from '../components/SeverityBadge';
import StatusBadge from '../components/StatusBadge';
import type { Client, Institution, Finding, WatchTerm, Severity } from '../types';
import { Building2, ChevronRight, ChevronDown, Tag, Globe, CreditCard, Hash, MapPin } from 'lucide-react';
import type { CSSProperties } from 'react';

const termIcons: Record<string, typeof Tag> = {
  name: Tag,
  domain: Globe,
  bin: CreditCard,
  routing_number: Hash,
};

export default function Institutions() {
  const [clients, setClients] = useState<Client[]>([]);
  const [institutions, setInstitutions] = useState<Institution[]>([]);
  const [expandedInst, setExpandedInst] = useState<string | null>(null);
  const [instFindings, setInstFindings] = useState<Finding[]>([]);
  const [instTerms, setInstTerms] = useState<WatchTerm[]>([]);
  const [loadingDetail, setLoadingDetail] = useState(false);

  useEffect(() => {
    fetchClients().then(setClients);
    fetchInstitutions().then(setInstitutions);
  }, []);

  const toggleInst = async (id: string) => {
    if (expandedInst === id) {
      setExpandedInst(null);
      return;
    }
    setExpandedInst(id);
    setLoadingDetail(true);
    const [findings, terms] = await Promise.all([
      fetchFindings({ institution_id: id }),
      fetchWatchTerms(id),
    ]);
    setInstFindings(findings);
    setInstTerms(terms);
    setLoadingDetail(false);
  };

  const grouped = clients.map(c => ({
    client: c,
    institutions: institutions.filter(i => i.client_id === c.id),
  }));

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 700, marginBottom: 4 }}>Institutions</h1>
      <p style={{ color: colors.textDim, fontSize: 14, marginBottom: 28 }}>
        {institutions.length} monitored institutions across {clients.length} clients
      </p>

      {grouped.map(({ client, institutions: insts }) => (
        <div key={client.id} style={{ marginBottom: 24 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12, paddingLeft: 4 }}>
            <Building2 size={16} color={colors.accent} />
            <h2 style={{ fontSize: 16, fontWeight: 600 }}>{client.name}</h2>
            <span style={{ fontSize: 12, color: colors.textMuted, marginLeft: 4 }}>{insts.length} institution{insts.length !== 1 ? 's' : ''}</span>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {insts.map(inst => {
              const isExpanded = expandedInst === inst.id;
              return (
                <div key={inst.id} style={{ ...card, padding: 0, overflow: 'hidden' }}>
                  <div
                    style={{
                      display: 'flex', alignItems: 'center', gap: 12, padding: '14px 20px', cursor: 'pointer',
                      transition: 'background 0.15s',
                    }}
                    onClick={() => toggleInst(inst.id)}
                    onMouseEnter={e => (e.currentTarget.style.background = colors.bgHover)}
                    onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                  >
                    {isExpanded ? <ChevronDown size={16} color={colors.textMuted} /> : <ChevronRight size={16} color={colors.textMuted} />}
                    <div style={{ flex: 1 }}>
                      <div style={{ fontWeight: 600, fontSize: 14 }}>{inst.name}</div>
                      <div style={{ fontSize: 12, color: colors.textMuted, display: 'flex', alignItems: 'center', gap: 4, marginTop: 2 }}>
                        <MapPin size={11} /> {inst.city}, {inst.state}
                        {inst.charter_number && <span style={{ marginLeft: 8, fontFamily: font.mono }}>{inst.charter_number}</span>}
                      </div>
                    </div>
                  </div>

                  {isExpanded && (
                    <div style={{ borderTop: `1px solid ${colors.border}`, padding: '16px 20px' }}>
                      {loadingDetail ? (
                        <div style={{ color: colors.textMuted, fontSize: 13 }}>Loading...</div>
                      ) : (
                        <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
                          {/* Watch Terms */}
                          <div style={{ flex: '1 1 280px', minWidth: 0 }}>
                            <h4 style={{ fontSize: 12, fontWeight: 600, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 10 }}>Watch Terms</h4>
                            {instTerms.length === 0 ? (
                              <div style={{ color: colors.textMuted, fontSize: 13 }}>No watch terms configured.</div>
                            ) : (
                              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                                {instTerms.map(t => {
                                  const Icon = termIcons[t.term_type] || Tag;
                                  return (
                                    <div key={t.id} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, color: colors.textDim }}>
                                      <Icon size={13} color={colors.textMuted} />
                                      <span style={{ fontFamily: font.mono, fontSize: 12, color: colors.text }}>{t.value}</span>
                                      <span style={{ fontSize: 10, color: colors.textMuted, background: colors.bgSurface, padding: '1px 6px', borderRadius: 3 }}>{t.term_type}</span>
                                    </div>
                                  );
                                })}
                              </div>
                            )}
                          </div>

                          {/* Findings summary */}
                          <div style={{ flex: '2 1 400px', minWidth: 0 }}>
                            <h4 style={{ fontSize: 12, fontWeight: 600, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 10 }}>
                              Findings ({instFindings.length})
                            </h4>
                            {instFindings.length === 0 ? (
                              <div style={{ color: colors.textMuted, fontSize: 13 }}>No findings for this institution.</div>
                            ) : (
                              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                                <thead>
                                  <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                                    {['Severity', 'Title', 'Status'].map(h => (
                                      <th key={h} style={{ textAlign: 'left', padding: '6px 8px', color: colors.textMuted, fontWeight: 500, fontSize: 10, textTransform: 'uppercase' }}>{h}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {instFindings.slice(0, 5).map(f => (
                                    <tr key={f.id} style={{ borderBottom: `1px solid ${colors.border}` }}>
                                      <td style={{ padding: '6px 8px' }}><SeverityBadge severity={f.severity} /></td>
                                      <td style={{ padding: '6px 8px', maxWidth: 280, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{f.title}</td>
                                      <td style={{ padding: '6px 8px' }}><StatusBadge status={f.status} /></td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            )}
                            {instFindings.length > 5 && (
                              <div style={{ fontSize: 11, color: colors.textMuted, marginTop: 6 }}>
                                + {instFindings.length - 5} more findings
                              </div>
                            )}
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}
