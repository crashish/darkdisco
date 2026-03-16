import { useEffect, useState, useCallback } from 'react';
import {
  fetchClients, fetchInstitutions, fetchFindings, fetchWatchTerms,
  createInstitution, updateInstitution, deleteInstitution,
  createWatchTerm, deleteWatchTerm,
} from '../api';
import { colors, card, font } from '../theme';
import SeverityBadge from '../components/SeverityBadge';
import StatusBadge from '../components/StatusBadge';
import type { Client, Institution, Finding, WatchTerm } from '../types';
import { Building2, ChevronRight, ChevronDown, Tag, Globe, CreditCard, Hash, MapPin, Plus, Pencil, Trash2, X, Check } from 'lucide-react';
import type { CSSProperties } from 'react';

const termIcons: Record<string, typeof Tag> = {
  name: Tag,
  domain: Globe,
  bin: CreditCard,
  routing_number: Hash,
};

const termTypes = ['name', 'domain', 'bin', 'routing_number'] as const;

const btnStyle: CSSProperties = {
  background: colors.accent,
  color: '#fff',
  border: 'none',
  borderRadius: 6,
  padding: '6px 14px',
  fontSize: 12,
  fontWeight: 600,
  cursor: 'pointer',
};

const btnDangerStyle: CSSProperties = {
  ...btnStyle,
  background: '#ef4444',
};

const btnGhostStyle: CSSProperties = {
  background: 'none',
  border: `1px solid ${colors.border}`,
  borderRadius: 6,
  padding: '6px 14px',
  fontSize: 12,
  color: colors.textDim,
  cursor: 'pointer',
};

const inputStyle: CSSProperties = {
  background: colors.bgSurface,
  border: `1px solid ${colors.border}`,
  borderRadius: 6,
  color: colors.text,
  padding: '6px 10px',
  fontSize: 13,
  outline: 'none',
  width: '100%',
};

export default function Institutions() {
  const [clients, setClients] = useState<Client[]>([]);
  const [institutions, setInstitutions] = useState<Institution[]>([]);
  const [expandedInst, setExpandedInst] = useState<string | null>(null);
  const [instFindings, setInstFindings] = useState<Finding[]>([]);
  const [instTerms, setInstTerms] = useState<WatchTerm[]>([]);
  const [loadingDetail, setLoadingDetail] = useState(false);

  // Edit state
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editForm, setEditForm] = useState({ name: '', city: '', state: '', charter_number: '' });

  // Add institution
  const [showAddForm, setShowAddForm] = useState(false);
  const [addForm, setAddForm] = useState({ client_id: '', name: '', city: '', state: '', charter_number: '' });

  // Add watch term
  const [showAddTerm, setShowAddTerm] = useState(false);
  const [termForm, setTermForm] = useState({ term_type: 'name', value: '' });

  const reload = useCallback(() => {
    fetchClients().then(setClients);
    fetchInstitutions().then(setInstitutions);
  }, []);

  useEffect(() => { reload(); }, [reload]);

  const toggleInst = async (id: string) => {
    if (expandedInst === id) {
      setExpandedInst(null);
      return;
    }
    setExpandedInst(id);
    setLoadingDetail(true);
    setShowAddTerm(false);
    const [findings, terms] = await Promise.all([
      fetchFindings({ institution_id: id }),
      fetchWatchTerms(id),
    ]);
    setInstFindings(findings.items);
    setInstTerms(terms);
    setLoadingDetail(false);
  };

  const startEdit = (inst: Institution) => {
    setEditingId(inst.id);
    setEditForm({
      name: inst.name,
      city: inst.city,
      state: inst.state,
      charter_number: inst.charter_number || '',
    });
  };

  const saveEdit = async () => {
    if (!editingId) return;
    await updateInstitution(editingId, editForm);
    setEditingId(null);
    reload();
  };

  const handleDelete = async (id: string, name: string) => {
    if (!confirm(`Delete institution "${name}"? This cannot be undone.`)) return;
    await deleteInstitution(id);
    if (expandedInst === id) setExpandedInst(null);
    reload();
  };

  const handleAddInstitution = async () => {
    if (!addForm.client_id || !addForm.name) return;
    await createInstitution(addForm);
    setShowAddForm(false);
    setAddForm({ client_id: '', name: '', city: '', state: '', charter_number: '' });
    reload();
  };

  const handleAddTerm = async () => {
    if (!expandedInst || !termForm.value.trim()) return;
    await createWatchTerm({ institution_id: expandedInst, term_type: termForm.term_type, value: termForm.value.trim() });
    setTermForm({ term_type: 'name', value: '' });
    setShowAddTerm(false);
    const terms = await fetchWatchTerms(expandedInst);
    setInstTerms(terms);
  };

  const handleDeleteTerm = async (termId: string) => {
    await deleteWatchTerm(termId);
    if (expandedInst) {
      const terms = await fetchWatchTerms(expandedInst);
      setInstTerms(terms);
    }
  };

  const grouped = clients.map(c => ({
    client: c,
    institutions: institutions.filter(i => i.client_id === c.id),
  }));

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 4 }}>
        <h1 style={{ fontSize: 24, fontWeight: 700 }}>Institutions</h1>
        <button style={btnStyle} onClick={() => { setShowAddForm(!showAddForm); if (!showAddForm && clients.length) setAddForm(f => ({ ...f, client_id: clients[0].id })); }}>
          <Plus size={14} style={{ verticalAlign: -2, marginRight: 4 }} /> Add Institution
        </button>
      </div>
      <p style={{ color: colors.textDim, fontSize: 14, marginBottom: 20 }}>
        {institutions.length} monitored institution{institutions.length !== 1 ? 's' : ''} across {clients.length} client{clients.length !== 1 ? 's' : ''}
      </p>

      {/* Add Institution Form */}
      {showAddForm && (
        <div style={{ ...card, marginBottom: 20, padding: 20 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>Add Institution</h3>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 12 }}>
            <div>
              <label style={{ fontSize: 11, color: colors.textMuted, display: 'block', marginBottom: 4 }}>Client</label>
              <select
                style={{ ...inputStyle, cursor: 'pointer', appearance: 'auto' as const }}
                value={addForm.client_id}
                onChange={e => setAddForm(f => ({ ...f, client_id: e.target.value }))}
              >
                {clients.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
            </div>
            <div>
              <label style={{ fontSize: 11, color: colors.textMuted, display: 'block', marginBottom: 4 }}>Name</label>
              <input style={inputStyle} value={addForm.name} onChange={e => setAddForm(f => ({ ...f, name: e.target.value }))} placeholder="Institution name" />
            </div>
            <div>
              <label style={{ fontSize: 11, color: colors.textMuted, display: 'block', marginBottom: 4 }}>City</label>
              <input style={inputStyle} value={addForm.city} onChange={e => setAddForm(f => ({ ...f, city: e.target.value }))} placeholder="City" />
            </div>
            <div>
              <label style={{ fontSize: 11, color: colors.textMuted, display: 'block', marginBottom: 4 }}>State</label>
              <input style={inputStyle} value={addForm.state} onChange={e => setAddForm(f => ({ ...f, state: e.target.value }))} placeholder="State" />
            </div>
            <div>
              <label style={{ fontSize: 11, color: colors.textMuted, display: 'block', marginBottom: 4 }}>Charter Number</label>
              <input style={inputStyle} value={addForm.charter_number} onChange={e => setAddForm(f => ({ ...f, charter_number: e.target.value }))} placeholder="Optional" />
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <button style={btnStyle} onClick={handleAddInstitution}>Create</button>
            <button style={btnGhostStyle} onClick={() => setShowAddForm(false)}>Cancel</button>
          </div>
        </div>
      )}

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
              const isEditing = editingId === inst.id;
              return (
                <div key={inst.id} style={{ ...card, padding: 0, overflow: 'hidden' }}>
                  <div
                    style={{
                      display: 'flex', alignItems: 'center', gap: 12, padding: '14px 20px', cursor: 'pointer',
                      transition: 'background 0.15s',
                    }}
                    onClick={() => !isEditing && toggleInst(inst.id)}
                    onMouseEnter={e => (e.currentTarget.style.background = colors.bgHover)}
                    onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                  >
                    {isExpanded ? <ChevronDown size={16} color={colors.textMuted} /> : <ChevronRight size={16} color={colors.textMuted} />}
                    <div style={{ flex: 1 }}>
                      {isEditing ? (
                        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }} onClick={e => e.stopPropagation()}>
                          <input style={{ ...inputStyle, width: 200 }} value={editForm.name} onChange={e => setEditForm(f => ({ ...f, name: e.target.value }))} />
                          <input style={{ ...inputStyle, width: 120 }} value={editForm.city} onChange={e => setEditForm(f => ({ ...f, city: e.target.value }))} placeholder="City" />
                          <input style={{ ...inputStyle, width: 60 }} value={editForm.state} onChange={e => setEditForm(f => ({ ...f, state: e.target.value }))} placeholder="ST" />
                          <input style={{ ...inputStyle, width: 100 }} value={editForm.charter_number} onChange={e => setEditForm(f => ({ ...f, charter_number: e.target.value }))} placeholder="Charter #" />
                          <button style={{ ...btnStyle, padding: '4px 8px' }} onClick={saveEdit} title="Save"><Check size={14} /></button>
                          <button style={{ ...btnGhostStyle, padding: '4px 8px' }} onClick={() => setEditingId(null)} title="Cancel"><X size={14} /></button>
                        </div>
                      ) : (
                        <>
                          <div style={{ fontWeight: 600, fontSize: 14 }}>{inst.name}</div>
                          <div style={{ fontSize: 12, color: colors.textMuted, display: 'flex', alignItems: 'center', gap: 4, marginTop: 2 }}>
                            <MapPin size={11} /> {inst.city}, {inst.state}
                            {inst.charter_number && <span style={{ marginLeft: 8, fontFamily: font.mono }}>{inst.charter_number}</span>}
                          </div>
                        </>
                      )}
                    </div>
                    {!isEditing && (
                      <div style={{ display: 'flex', gap: 4 }} onClick={e => e.stopPropagation()}>
                        <button
                          style={{ ...btnGhostStyle, padding: '4px 8px', border: 'none' }}
                          onClick={() => startEdit(inst)}
                          title="Edit"
                        >
                          <Pencil size={13} color={colors.textMuted} />
                        </button>
                        <button
                          style={{ ...btnGhostStyle, padding: '4px 8px', border: 'none' }}
                          onClick={() => handleDelete(inst.id, inst.name)}
                          title="Delete"
                        >
                          <Trash2 size={13} color={colors.textMuted} />
                        </button>
                      </div>
                    )}
                  </div>

                  {isExpanded && (
                    <div style={{ borderTop: `1px solid ${colors.border}`, padding: '16px 20px' }}>
                      {loadingDetail ? (
                        <div style={{ color: colors.textMuted, fontSize: 13 }}>Loading...</div>
                      ) : (
                        <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
                          {/* Watch Terms */}
                          <div style={{ flex: '1 1 280px', minWidth: 0 }}>
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
                              <h4 style={{ fontSize: 12, fontWeight: 600, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.05em' }}>Watch Terms</h4>
                              <button
                                style={{ ...btnGhostStyle, padding: '2px 8px', fontSize: 11 }}
                                onClick={() => setShowAddTerm(!showAddTerm)}
                              >
                                <Plus size={11} style={{ verticalAlign: -1, marginRight: 2 }} /> Add
                              </button>
                            </div>

                            {showAddTerm && (
                              <div style={{ display: 'flex', gap: 6, marginBottom: 10, alignItems: 'center' }}>
                                <select
                                  style={{ ...inputStyle, width: 120, cursor: 'pointer', appearance: 'auto' as const }}
                                  value={termForm.term_type}
                                  onChange={e => setTermForm(f => ({ ...f, term_type: e.target.value }))}
                                >
                                  {termTypes.map(t => <option key={t} value={t}>{t}</option>)}
                                </select>
                                <input
                                  style={{ ...inputStyle, flex: 1 }}
                                  value={termForm.value}
                                  onChange={e => setTermForm(f => ({ ...f, value: e.target.value }))}
                                  placeholder="Term value..."
                                  onKeyDown={e => e.key === 'Enter' && handleAddTerm()}
                                />
                                <button style={{ ...btnStyle, padding: '4px 10px' }} onClick={handleAddTerm}>
                                  <Check size={13} />
                                </button>
                              </div>
                            )}

                            {instTerms.length === 0 && !showAddTerm ? (
                              <div style={{ color: colors.textMuted, fontSize: 13 }}>No watch terms configured.</div>
                            ) : (
                              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                                {instTerms.map(t => {
                                  const Icon = termIcons[t.term_type] || Tag;
                                  return (
                                    <div key={t.id} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, color: colors.textDim }}>
                                      <Icon size={13} color={colors.textMuted} />
                                      <span style={{ fontFamily: font.mono, fontSize: 12, color: colors.text, flex: 1 }}>{t.value}</span>
                                      <span style={{ fontSize: 10, color: colors.textMuted, background: colors.bgSurface, padding: '1px 6px', borderRadius: 3 }}>{t.term_type}</span>
                                      <button
                                        style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 2 }}
                                        onClick={() => handleDeleteTerm(t.id)}
                                        title="Remove term"
                                      >
                                        <X size={12} color={colors.textMuted} />
                                      </button>
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
