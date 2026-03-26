import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { fetchAlertRules, createAlertRule, updateAlertRule, deleteAlertRule, fetchInstitutions } from '../api';
import type { AlertRule, AlertRuleCreate, AlertRuleUpdate, Institution, Severity, SourceType } from '../types';
import { colors, card, font, badge, severityColor, severityBg } from '../theme';
import { Bell, Plus, Trash2, Pencil, X, Check, Power, PowerOff, Mail, MessageSquare, Globe, ChevronDown, History } from 'lucide-react';
import type { CSSProperties } from 'react';

const SEVERITIES: Severity[] = ['critical', 'high', 'medium', 'low', 'info'];
const SOURCE_TYPES: SourceType[] = [
  'paste_site', 'forum', 'marketplace', 'telegram', 'telegram_intel',
  'discord', 'breach_db', 'ransomware_blog', 'stealer_log', 'other',
];

const sectionStyle: CSSProperties = { ...card, marginBottom: 16 };

const inputStyle: CSSProperties = {
  background: colors.bgSurface,
  border: `1px solid ${colors.border}`,
  borderRadius: 6,
  padding: '8px 12px',
  color: colors.text,
  fontSize: 13,
  fontFamily: font.sans,
  outline: 'none',
  width: '100%',
};

const selectStyle: CSSProperties = {
  ...inputStyle,
  cursor: 'pointer',
  appearance: 'none' as const,
  backgroundImage: 'none',
};

const btnStyle: CSSProperties = {
  background: colors.accent,
  color: '#fff',
  border: 'none',
  borderRadius: 6,
  padding: '10px 20px',
  fontSize: 14,
  fontWeight: 600,
  cursor: 'pointer',
  display: 'inline-flex',
  alignItems: 'center',
  gap: 8,
};

const btnSecondary: CSSProperties = {
  ...btnStyle,
  background: colors.bgSurface,
  color: colors.textDim,
  border: `1px solid ${colors.border}`,
};

const btnDanger: CSSProperties = {
  ...btnStyle,
  background: 'transparent',
  color: colors.critical,
  padding: '6px 8px',
};

const labelStyle: CSSProperties = {
  fontSize: 12,
  fontWeight: 600,
  color: colors.textDim,
  textTransform: 'uppercase' as const,
  letterSpacing: '0.05em',
  marginBottom: 6,
  display: 'block',
};

const checkboxWrap: CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  fontSize: 13,
  color: colors.text,
  cursor: 'pointer',
};

interface RuleFormData {
  name: string;
  institution_id: string;
  min_severity: Severity;
  source_types: string[];
  keyword_filter: string;
  enabled: boolean;
  notify_email: boolean;
  notify_slack: boolean;
  notify_webhook_url: string;
}

const emptyForm: RuleFormData = {
  name: '',
  institution_id: '',
  min_severity: 'high',
  source_types: [],
  keyword_filter: '',
  enabled: true,
  notify_email: false,
  notify_slack: false,
  notify_webhook_url: '',
};

function RuleForm({
  initial,
  institutions,
  onSubmit,
  onCancel,
  submitLabel,
}: {
  initial: RuleFormData;
  institutions: Institution[];
  onSubmit: (data: RuleFormData) => void;
  onCancel: () => void;
  submitLabel: string;
}) {
  const [form, setForm] = useState<RuleFormData>(initial);
  const [showSourceTypes, setShowSourceTypes] = useState(initial.source_types.length > 0);

  const update = (patch: Partial<RuleFormData>) => setForm(f => ({ ...f, ...patch }));

  const toggleSourceType = (st: string) => {
    const next = form.source_types.includes(st)
      ? form.source_types.filter(s => s !== st)
      : [...form.source_types, st];
    update({ source_types: next });
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {/* Name */}
      <div>
        <label style={labelStyle}>Rule Name *</label>
        <input
          value={form.name}
          onChange={e => update({ name: e.target.value })}
          style={inputStyle}
          placeholder="e.g. Critical findings for Revolut"
        />
      </div>

      {/* Row: Institution + Severity */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
        <div>
          <label style={labelStyle}>Institution (optional)</label>
          <div style={{ position: 'relative' }}>
            <select
              value={form.institution_id}
              onChange={e => update({ institution_id: e.target.value })}
              style={selectStyle}
            >
              <option value="">All institutions</option>
              {institutions.map(inst => (
                <option key={inst.id} value={inst.id}>{inst.name}</option>
              ))}
            </select>
            <ChevronDown size={14} style={{ position: 'absolute', right: 12, top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none', color: colors.textMuted }} />
          </div>
        </div>
        <div>
          <label style={labelStyle}>Minimum Severity</label>
          <div style={{ position: 'relative' }}>
            <select
              value={form.min_severity}
              onChange={e => update({ min_severity: e.target.value as Severity })}
              style={selectStyle}
            >
              {SEVERITIES.map(s => (
                <option key={s} value={s}>{s.charAt(0).toUpperCase() + s.slice(1)}</option>
              ))}
            </select>
            <ChevronDown size={14} style={{ position: 'absolute', right: 12, top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none', color: colors.textMuted }} />
          </div>
        </div>
      </div>

      {/* Keyword filter */}
      <div>
        <label style={labelStyle}>Keyword Filter (optional)</label>
        <input
          value={form.keyword_filter}
          onChange={e => update({ keyword_filter: e.target.value })}
          style={inputStyle}
          placeholder="e.g. BIN, credential, fullz"
        />
        <div style={{ fontSize: 11, color: colors.textMuted, marginTop: 4 }}>
          Substring match against finding title + summary
        </div>
      </div>

      {/* Source types */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
          <label style={{ ...labelStyle, marginBottom: 0 }}>Source Types</label>
          <label style={checkboxWrap}>
            <input
              type="checkbox"
              checked={showSourceTypes}
              onChange={e => {
                setShowSourceTypes(e.target.checked);
                if (!e.target.checked) update({ source_types: [] });
              }}
              style={{ accentColor: colors.accent }}
            />
            <span style={{ fontSize: 12, color: colors.textMuted }}>Filter by source type</span>
          </label>
        </div>
        {showSourceTypes && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {SOURCE_TYPES.map(st => {
              const active = form.source_types.includes(st);
              return (
                <button
                  key={st}
                  onClick={() => toggleSourceType(st)}
                  style={{
                    ...badge(
                      active ? colors.accent : colors.textMuted,
                      active ? 'rgba(99, 102, 241, 0.15)' : colors.bgSurface,
                    ),
                    border: `1px solid ${active ? colors.accent : colors.border}`,
                    cursor: 'pointer',
                    fontFamily: font.sans,
                  }}
                >
                  {st.replace(/_/g, ' ')}
                </button>
              );
            })}
          </div>
        )}
      </div>

      {/* Notification channels */}
      <div>
        <label style={labelStyle}>Notification Channels</label>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          <label style={checkboxWrap}>
            <input
              type="checkbox"
              checked={form.notify_email}
              onChange={e => update({ notify_email: e.target.checked })}
              style={{ accentColor: colors.accent }}
            />
            <Mail size={14} />
            Email notification
          </label>
          <label style={checkboxWrap}>
            <input
              type="checkbox"
              checked={form.notify_slack}
              onChange={e => update({ notify_slack: e.target.checked })}
              style={{ accentColor: colors.accent }}
            />
            <MessageSquare size={14} />
            Slack webhook
          </label>
          <div>
            <label style={checkboxWrap}>
              <input
                type="checkbox"
                checked={form.notify_webhook_url.length > 0}
                onChange={e => {
                  if (!e.target.checked) update({ notify_webhook_url: '' });
                }}
                style={{ accentColor: colors.accent }}
              />
              <Globe size={14} />
              Custom webhook URL
            </label>
            {form.notify_webhook_url !== '' || form.notify_webhook_url === '' ? (
              <input
                value={form.notify_webhook_url}
                onChange={e => update({ notify_webhook_url: e.target.value })}
                style={{ ...inputStyle, marginTop: 6, marginLeft: 30 }}
                placeholder="https://example.com/webhook"
              />
            ) : null}
          </div>
        </div>
      </div>

      {/* Enabled toggle */}
      <label style={checkboxWrap}>
        <input
          type="checkbox"
          checked={form.enabled}
          onChange={e => update({ enabled: e.target.checked })}
          style={{ accentColor: colors.accent }}
        />
        Rule enabled
      </label>

      {/* Actions */}
      <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
        <button onClick={onCancel} style={btnSecondary}>
          <X size={14} /> Cancel
        </button>
        <button
          onClick={() => onSubmit(form)}
          disabled={!form.name.trim()}
          style={{
            ...btnStyle,
            opacity: !form.name.trim() ? 0.5 : 1,
            cursor: !form.name.trim() ? 'default' : 'pointer',
          }}
        >
          <Check size={14} /> {submitLabel}
        </button>
      </div>
    </div>
  );
}

export default function AlertRulesPage() {
  const navigate = useNavigate();
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [institutions, setInstitutions] = useState<Institution[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);

  const load = async () => {
    try {
      const [r, i] = await Promise.all([fetchAlertRules(), fetchInstitutions()]);
      setRules(r);
      setInstitutions(i);
    } catch (e) {
      setError(`Failed to load: ${(e as Error).message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const handleCreate = async (data: RuleFormData) => {
    try {
      const body: AlertRuleCreate = {
        name: data.name,
        owner_id: 'current-user',
        min_severity: data.min_severity,
        enabled: data.enabled,
        notify_email: data.notify_email,
        notify_slack: data.notify_slack,
      };
      if (data.institution_id) body.institution_id = data.institution_id;
      if (data.source_types.length > 0) body.source_types = data.source_types;
      if (data.keyword_filter.trim()) body.keyword_filter = data.keyword_filter;
      if (data.notify_webhook_url.trim()) body.notify_webhook_url = data.notify_webhook_url;

      await createAlertRule(body);
      setShowCreate(false);
      await load();
    } catch (e) {
      setError(`Failed to create rule: ${(e as Error).message}`);
    }
  };

  const handleUpdate = async (id: string, data: RuleFormData) => {
    try {
      const body: AlertRuleUpdate = {
        name: data.name,
        institution_id: data.institution_id || null,
        min_severity: data.min_severity,
        source_types: data.source_types.length > 0 ? data.source_types : null,
        keyword_filter: data.keyword_filter.trim() || null,
        enabled: data.enabled,
        notify_email: data.notify_email,
        notify_slack: data.notify_slack,
        notify_webhook_url: data.notify_webhook_url.trim() || null,
      };
      await updateAlertRule(id, body);
      setEditingId(null);
      await load();
    } catch (e) {
      setError(`Failed to update rule: ${(e as Error).message}`);
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteAlertRule(id);
      await load();
    } catch (e) {
      setError(`Failed to delete: ${(e as Error).message}`);
    }
  };

  const handleToggle = async (rule: AlertRule) => {
    try {
      await updateAlertRule(rule.id, { enabled: !rule.enabled });
      await load();
    } catch (e) {
      setError(`Failed to toggle: ${(e as Error).message}`);
    }
  };

  const instName = (id: string | null) => {
    if (!id) return 'All';
    return institutions.find(i => i.id === id)?.name || id.slice(0, 8);
  };

  if (loading) {
    return <div style={{ padding: 40, color: colors.textDim }}>Loading alert rules...</div>;
  }

  return (
    <div>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <h1 style={{ fontSize: 24, fontWeight: 700, color: colors.text, margin: 0, display: 'flex', alignItems: 'center', gap: 10 }}>
            <Bell size={24} color={colors.accent} />
            Alert Rules
          </h1>
          <p style={{ fontSize: 14, color: colors.textDim, margin: '6px 0 0' }}>
            Configure automated alerts when findings match specific criteria.
          </p>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button onClick={() => navigate('/settings/alert-history')} style={btnSecondary}>
            <History size={16} /> Alert History
          </button>
          <button onClick={() => { setShowCreate(true); setEditingId(null); }} style={btnStyle}>
            <Plus size={16} /> New Rule
          </button>
        </div>
      </div>

      {error && (
        <div style={{ ...sectionStyle, background: 'rgba(239, 68, 68, 0.08)', borderColor: colors.critical, color: colors.critical, fontSize: 14, display: 'flex', alignItems: 'center', gap: 8 }}>
          <X size={16} /> {error}
          <button onClick={() => setError(null)} style={{ ...btnDanger, marginLeft: 'auto' }}><X size={14} /></button>
        </div>
      )}

      {/* Create form */}
      {showCreate && (
        <div style={sectionStyle}>
          <div style={{ fontSize: 15, fontWeight: 600, color: colors.text, marginBottom: 16, display: 'flex', alignItems: 'center', gap: 8 }}>
            <Plus size={16} color={colors.accent} /> Create Alert Rule
          </div>
          <RuleForm
            initial={emptyForm}
            institutions={institutions}
            onSubmit={handleCreate}
            onCancel={() => setShowCreate(false)}
            submitLabel="Create Rule"
          />
        </div>
      )}

      {/* Rules list */}
      {rules.length === 0 && !showCreate ? (
        <div style={{ ...sectionStyle, textAlign: 'center', padding: 40, color: colors.textDim }}>
          <Bell size={32} style={{ marginBottom: 12, opacity: 0.4 }} />
          <div style={{ fontSize: 15, marginBottom: 8 }}>No alert rules configured</div>
          <div style={{ fontSize: 13, marginBottom: 16 }}>Create your first rule to get notified when findings match specific criteria.</div>
          <button onClick={() => setShowCreate(true)} style={btnStyle}>
            <Plus size={16} /> Create First Rule
          </button>
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {rules.map(rule => (
            <div key={rule.id} style={sectionStyle}>
              {editingId === rule.id ? (
                <RuleForm
                  initial={{
                    name: rule.name,
                    institution_id: rule.institution_id || '',
                    min_severity: rule.min_severity,
                    source_types: rule.source_types || [],
                    keyword_filter: rule.keyword_filter || '',
                    enabled: rule.enabled,
                    notify_email: rule.notify_email,
                    notify_slack: rule.notify_slack,
                    notify_webhook_url: rule.notify_webhook_url || '',
                  }}
                  institutions={institutions}
                  onSubmit={(data) => handleUpdate(rule.id, data)}
                  onCancel={() => setEditingId(null)}
                  submitLabel="Save Changes"
                />
              ) : (
                <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12 }}>
                  {/* Status indicator */}
                  <div style={{
                    width: 8,
                    height: 8,
                    borderRadius: '50%',
                    background: rule.enabled ? colors.healthy : colors.textMuted,
                    marginTop: 6,
                    flexShrink: 0,
                  }} />

                  {/* Content */}
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                      <span style={{ fontSize: 15, fontWeight: 600, color: rule.enabled ? colors.text : colors.textMuted }}>
                        {rule.name}
                      </span>
                      {!rule.enabled && (
                        <span style={badge(colors.textMuted, colors.bgSurface)}>disabled</span>
                      )}
                    </div>

                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, fontSize: 12 }}>
                      {/* Institution */}
                      <span style={badge(colors.accent, 'rgba(99, 102, 241, 0.12)')}>
                        {instName(rule.institution_id)}
                      </span>

                      {/* Severity */}
                      <span style={badge(severityColor(rule.min_severity), severityBg(rule.min_severity))}>
                        {'>='} {rule.min_severity}
                      </span>

                      {/* Source types */}
                      {rule.source_types && rule.source_types.length > 0 && (
                        <span style={badge(colors.textDim, colors.bgSurface)}>
                          {rule.source_types.length} source type{rule.source_types.length > 1 ? 's' : ''}
                        </span>
                      )}

                      {/* Keyword */}
                      {rule.keyword_filter && (
                        <span style={{ ...badge(colors.textDim, colors.bgSurface), fontFamily: font.mono }}>
                          "{rule.keyword_filter}"
                        </span>
                      )}

                      {/* Channels */}
                      {rule.notify_email && (
                        <span style={badge(colors.healthy, 'rgba(34, 197, 94, 0.12)')}>
                          <Mail size={10} /> email
                        </span>
                      )}
                      {rule.notify_slack && (
                        <span style={badge(colors.healthy, 'rgba(34, 197, 94, 0.12)')}>
                          <MessageSquare size={10} /> slack
                        </span>
                      )}
                      {rule.notify_webhook_url && (
                        <span style={badge(colors.healthy, 'rgba(34, 197, 94, 0.12)')}>
                          <Globe size={10} /> webhook
                        </span>
                      )}
                    </div>

                    <div style={{ fontSize: 11, color: colors.textMuted, marginTop: 6 }}>
                      Created {new Date(rule.created_at).toLocaleDateString()}
                    </div>
                  </div>

                  {/* Actions */}
                  <div style={{ display: 'flex', gap: 4, flexShrink: 0 }}>
                    <button
                      onClick={() => handleToggle(rule)}
                      title={rule.enabled ? 'Disable' : 'Enable'}
                      style={{
                        ...btnDanger,
                        color: rule.enabled ? colors.healthy : colors.textMuted,
                      }}
                    >
                      {rule.enabled ? <Power size={14} /> : <PowerOff size={14} />}
                    </button>
                    <button
                      onClick={() => { setEditingId(rule.id); setShowCreate(false); }}
                      style={{ ...btnDanger, color: colors.textDim }}
                      title="Edit"
                    >
                      <Pencil size={14} />
                    </button>
                    <button
                      onClick={() => handleDelete(rule.id)}
                      style={btnDanger}
                      title="Delete"
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
