import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { fetchSource, updateSource, triggerSourcePoll } from '../api';
import { colors, card, font, healthColor, healthBg, badge as makeBadge } from '../theme';
import type { SourceDetail as SourceDetailType, SourceHealth } from '../types';
import {
  ArrowLeft, Settings, Play, Clock, Hash, Activity, AlertTriangle,
  Wifi, WifiOff, CheckCircle, XCircle, RefreshCw, Save, X,
} from 'lucide-react';
import type { CSSProperties } from 'react';

const healthIcons: Record<SourceHealth, typeof Wifi> = {
  healthy: Wifi,
  degraded: AlertTriangle,
  offline: WifiOff,
};

const sourceLabels: Record<string, string> = {
  tor_forum: 'Tor Forum',
  paste_site: 'Paste Site',
  telegram: 'Telegram',
  breach_db: 'Breach Database',
  ransomware_blog: 'Ransomware Blog',
};

const sectionStyle: CSSProperties = {
  ...card,
  marginBottom: 16,
};

const sectionTitle: CSSProperties = {
  fontSize: 13,
  fontWeight: 600,
  color: colors.textDim,
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
  marginBottom: 12,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
};

const labelStyle: CSSProperties = {
  fontSize: 11,
  color: colors.textMuted,
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
  marginBottom: 4,
};

const valueStyle: CSSProperties = {
  fontSize: 13,
  color: colors.text,
};

const btnBase: CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: 6,
  padding: '8px 16px',
  borderRadius: 6,
  fontSize: 13,
  fontWeight: 600,
  cursor: 'pointer',
  border: 'none',
  transition: 'opacity 0.15s',
};

export default function SourceDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [source, setSource] = useState<SourceDetailType | null>(null);
  const [loading, setLoading] = useState(true);
  const [polling, setPolling] = useState(false);
  const [editingConfig, setEditingConfig] = useState(false);
  const [configDraft, setConfigDraft] = useState('');
  const [configError, setConfigError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    fetchSource(id).then(s => { setSource(s); setLoading(false); });
  }, [id]);

  const handlePoll = async () => {
    if (!source || polling) return;
    setPolling(true);
    try {
      const result = await triggerSourcePoll(source.id);
      setSource(prev => prev ? { ...prev, last_polled_at: result.polled_at, last_error: null } : prev);
    } finally {
      setPolling(false);
    }
  };

  const startEditConfig = () => {
    setConfigDraft(JSON.stringify(source?.config ?? {}, null, 2));
    setConfigError(null);
    setEditingConfig(true);
  };

  const saveConfig = async () => {
    if (!source) return;
    try {
      const parsed = JSON.parse(configDraft);
      setSaving(true);
      const updated = await updateSource(source.id, { config: parsed });
      setSource(prev => prev ? { ...prev, config: updated.config } : prev);
      setEditingConfig(false);
      setConfigError(null);
    } catch (e) {
      setConfigError(e instanceof SyntaxError ? 'Invalid JSON' : 'Save failed');
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return <div style={{ color: colors.textMuted, padding: 40, textAlign: 'center' }}>Loading...</div>;
  }

  if (!source) {
    return <div style={{ color: colors.textMuted, padding: 40, textAlign: 'center' }}>Source not found.</div>;
  }

  const HealthIcon = healthIcons[source.health];
  const hColor = healthColor(source.health);
  const hBg = healthBg(source.health);
  const maxFindings = Math.max(...source.findings_by_day.map(d => d.count), 1);

  return (
    <div>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <button
          onClick={() => navigate('/sources')}
          style={{
            background: 'none', border: 'none', color: colors.accent,
            fontSize: 13, cursor: 'pointer', padding: 0, marginBottom: 16,
            display: 'inline-flex', alignItems: 'center', gap: 6,
          }}
        >
          <ArrowLeft size={14} /> Back to Sources
        </button>

        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 16 }}>
          <div style={{ flex: 1 }}>
            <h1 style={{ fontSize: 22, fontWeight: 700, marginBottom: 8, lineHeight: 1.3 }}>{source.name}</h1>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
              <span style={makeBadge(hColor, hBg)}>
                <HealthIcon size={12} />
                {source.health}
              </span>
              <span style={{ fontSize: 11, color: colors.textMuted, fontFamily: font.mono }}>
                {sourceLabels[source.source_type] || source.source_type}
              </span>
              {!source.enabled && (
                <span style={makeBadge(colors.textMuted, colors.bgSurface)}>disabled</span>
              )}
            </div>
            <div style={{ fontSize: 11, color: colors.textMuted, marginTop: 8 }}>
              ID: {source.id} &middot; Created: {new Date(source.created_at).toLocaleDateString()}
              {source.connector_class && <> &middot; {source.connector_class}</>}
            </div>
          </div>

          <button
            onClick={handlePoll}
            disabled={polling || !source.enabled}
            style={{
              ...btnBase,
              background: source.enabled ? colors.accent : colors.bgSurface,
              color: source.enabled ? '#fff' : colors.textMuted,
              opacity: polling ? 0.6 : 1,
            }}
          >
            {polling ? <RefreshCw size={14} style={{ animation: 'spin 1s linear infinite' }} /> : <Play size={14} />}
            {polling ? 'Polling...' : 'Poll Now'}
          </button>
        </div>
      </div>

      {/* Metrics row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 16 }}>
        <MetricCard icon={<Clock size={16} color={colors.textMuted} />} label="Last Polled" value={source.last_polled_at ? timeAgo(source.last_polled_at) : 'Never'} />
        <MetricCard icon={<Hash size={16} color={colors.textMuted} />} label="Total Findings" value={source.finding_count.toLocaleString()} />
        <MetricCard icon={<Activity size={16} color={colors.textMuted} />} label="Poll Interval" value={formatInterval(source.poll_interval_seconds)} />
        <MetricCard icon={<HealthIcon size={16} color={hColor} />} label="Health" value={source.health} color={hColor} />
      </div>

      {/* Error banner */}
      {source.last_error && (
        <div style={{
          ...card,
          marginBottom: 16,
          borderColor: colors.offline,
          background: colors.offlineBg,
          display: 'flex',
          alignItems: 'center',
          gap: 12,
        }}>
          <XCircle size={16} color={colors.offline} />
          <div>
            <div style={{ fontSize: 12, fontWeight: 600, color: colors.offline, marginBottom: 2 }}>Last Error</div>
            <div style={{ fontSize: 13, color: colors.text }}>{source.last_error}</div>
          </div>
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 340px', gap: 16, alignItems: 'start' }}>
        {/* Left column */}
        <div>
          {/* Findings chart */}
          <div style={sectionStyle}>
            <div style={sectionTitle}><Activity size={14} /> Findings (14 Days)</div>
            <div style={{ display: 'flex', alignItems: 'flex-end', gap: 4, height: 100 }}>
              {source.findings_by_day.map((d, i) => (
                <div key={i} style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4 }}>
                  <div
                    style={{
                      width: '100%',
                      height: `${(d.count / maxFindings) * 80}px`,
                      minHeight: 2,
                      background: colors.accent,
                      borderRadius: 2,
                      opacity: 0.8,
                    }}
                    title={`${d.date}: ${d.count} findings`}
                  />
                  {i % 2 === 0 && (
                    <span style={{ fontSize: 9, color: colors.textMuted, whiteSpace: 'nowrap' }}>
                      {d.date.slice(5)}
                    </span>
                  )}
                </div>
              ))}
            </div>
          </div>

          {/* Poll History */}
          <div style={sectionStyle}>
            <div style={sectionTitle}><Clock size={14} /> Poll History</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
              {source.poll_history.map((entry, i) => {
                const isError = entry.status === 'error';
                return (
                  <div key={i} style={{
                    display: 'flex', alignItems: 'center', gap: 12, padding: '8px 0',
                    borderBottom: i < source.poll_history.length - 1 ? `1px solid ${colors.border}` : 'none',
                  }}>
                    {isError
                      ? <XCircle size={14} color={colors.offline} />
                      : <CheckCircle size={14} color={colors.healthy} />
                    }
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 12, color: colors.text }}>
                        {new Date(entry.polled_at).toLocaleString()}
                      </div>
                      {isError && entry.error && (
                        <div style={{ fontSize: 11, color: colors.offline, marginTop: 2 }}>{entry.error}</div>
                      )}
                    </div>
                    <div style={{ fontSize: 11, color: colors.textMuted, fontFamily: font.mono, minWidth: 60, textAlign: 'right' }}>
                      {entry.duration_ms}ms
                    </div>
                    <div style={{ fontSize: 11, color: isError ? colors.offline : colors.textDim, fontFamily: font.mono, minWidth: 40, textAlign: 'right' }}>
                      {entry.findings_found} hits
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* Right column */}
        <div>
          {/* Configuration */}
          <div style={sectionStyle}>
            <div style={{ ...sectionTitle, justifyContent: 'space-between' }}>
              <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <Settings size={14} /> Configuration
              </span>
              {!editingConfig && (
                <button
                  onClick={startEditConfig}
                  style={{
                    background: 'none', border: `1px solid ${colors.border}`, color: colors.textDim,
                    fontSize: 11, padding: '3px 10px', borderRadius: 4, cursor: 'pointer',
                  }}
                >
                  Edit
                </button>
              )}
            </div>

            {editingConfig ? (
              <div>
                <textarea
                  value={configDraft}
                  onChange={e => { setConfigDraft(e.target.value); setConfigError(null); }}
                  style={{
                    width: '100%',
                    minHeight: 200,
                    fontFamily: font.mono,
                    fontSize: 12,
                    lineHeight: 1.6,
                    background: colors.bgSurface,
                    color: colors.text,
                    border: `1px solid ${configError ? colors.offline : colors.border}`,
                    borderRadius: 6,
                    padding: 12,
                    resize: 'vertical',
                    outline: 'none',
                    boxSizing: 'border-box',
                  }}
                />
                {configError && (
                  <div style={{ fontSize: 11, color: colors.offline, marginTop: 4 }}>{configError}</div>
                )}
                <div style={{ display: 'flex', gap: 8, marginTop: 8 }}>
                  <button
                    onClick={saveConfig}
                    disabled={saving}
                    style={{ ...btnBase, background: colors.accent, color: '#fff', fontSize: 12, padding: '6px 14px' }}
                  >
                    <Save size={12} /> {saving ? 'Saving...' : 'Save'}
                  </button>
                  <button
                    onClick={() => setEditingConfig(false)}
                    style={{ ...btnBase, background: colors.bgSurface, color: colors.textDim, fontSize: 12, padding: '6px 14px' }}
                  >
                    <X size={12} /> Cancel
                  </button>
                </div>
              </div>
            ) : (
              <pre style={{
                fontFamily: font.mono, fontSize: 12, lineHeight: 1.6,
                color: colors.textDim, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
                background: colors.bgSurface, padding: 12, borderRadius: 6,
                border: `1px solid ${colors.border}`, margin: 0,
              }}>
                {source.config ? JSON.stringify(source.config, null, 2) : 'No configuration'}
              </pre>
            )}
          </div>

          {/* Source Info */}
          <div style={sectionStyle}>
            <div style={sectionTitle}><Activity size={14} /> Details</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              <div>
                <div style={labelStyle}>Type</div>
                <div style={valueStyle}>{sourceLabels[source.source_type] || source.source_type}</div>
              </div>
              <div>
                <div style={labelStyle}>Connector</div>
                <div style={{ ...valueStyle, fontFamily: font.mono, fontSize: 11, wordBreak: 'break-all' }}>
                  {source.connector_class || 'Not configured'}
                </div>
              </div>
              {source.url && (
                <div>
                  <div style={labelStyle}>URL</div>
                  <div style={{ ...valueStyle, fontFamily: font.mono, fontSize: 11, wordBreak: 'break-all' }}>
                    {source.url}
                  </div>
                </div>
              )}
              <div>
                <div style={labelStyle}>Enabled</div>
                <div style={{ ...valueStyle, color: source.enabled ? colors.healthy : colors.offline }}>
                  {source.enabled ? 'Yes' : 'No'}
                </div>
              </div>
              <div>
                <div style={labelStyle}>Poll Interval</div>
                <div style={valueStyle}>{formatInterval(source.poll_interval_seconds)}</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <style>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
}

function MetricCard({ icon, label, value, color }: { icon: React.ReactNode; label: string; value: string; color?: string }) {
  return (
    <div style={card}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
        {icon}
        <span style={{ fontSize: 10, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{label}</span>
      </div>
      <div style={{ fontSize: 18, fontWeight: 700, color: color || colors.text, fontFamily: font.mono }}>
        {value}
      </div>
    </div>
  );
}

function timeAgo(iso: string): string {
  const ms = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(ms / 60000);
  if (mins < 1) return 'Just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

function formatInterval(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}
