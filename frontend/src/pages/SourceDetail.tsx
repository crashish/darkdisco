import { useEffect, useState, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import {
  fetchSource, fetchChannels, addChannel, removeChannel,
  updateSource, triggerPoll, fetchSourceFindings, fetchSourceFindingsTrend,
} from '../api';
import { colors, card, font, badge as makeBadge, healthColor, healthBg, severityColor, severityBg } from '../theme';
import type { Source, TelegramChannel, Finding, FindingTrend } from '../types';
import {
  ArrowLeft, Plus, Trash2, MessageSquare, Hash, Wifi, WifiOff,
  AlertTriangle, Loader, Play, Settings, Activity, BarChart3, Save, Check,
  Code, Eye, EyeOff, X,
} from 'lucide-react';
import type { CSSProperties } from 'react';

const sourceLabels: Record<string, string> = {
  tor_forum: 'Tor Forum',
  paste_site: 'Paste Site',
  telegram: 'Telegram',
  breach_db: 'Breach Database',
  ransomware_blog: 'Ransomware Blog',
  stealer_log: 'Stealer Log',
  forum: 'Forum',
  marketplace: 'Marketplace',
  other: 'Other',
};

const healthIcons: Record<string, typeof Wifi> = {
  healthy: Wifi,
  degraded: AlertTriangle,
  offline: WifiOff,
};

interface ConfigFieldDef {
  key: string;
  label: string;
  type: 'list' | 'readonly-count' | 'readonly-map' | 'key-value';
  placeholder?: string;
  masked?: boolean;
}

const sourceConfigFields: Record<string, ConfigFieldDef[]> = {
  telegram: [
    { key: 'last_message_ids', label: 'High-Water Marks', type: 'readonly-map' },
  ],
  paste_site: [
    { key: 'paste_sites', label: 'Paste Sites', type: 'list', placeholder: 'https://pastebin.com/...' },
    { key: 'seen_hashes', label: 'Seen Hashes', type: 'readonly-count' },
  ],
  forum: [
    { key: 'forums', label: 'Forums', type: 'list', placeholder: 'forum URL or name' },
    { key: 'last_thread_ids', label: 'Thread Tracking', type: 'readonly-map' },
  ],
  tor_forum: [
    { key: 'forums', label: 'Forums', type: 'list', placeholder: 'forum URL or .onion' },
    { key: 'last_thread_ids', label: 'Thread Tracking', type: 'readonly-map' },
  ],
  breach_db: [
    { key: 'services', label: 'Services', type: 'list', placeholder: 'service name' },
    { key: 'api_keys', label: 'API Keys', type: 'key-value', masked: true },
  ],
  ransomware_blog: [
    { key: 'groups', label: 'Groups', type: 'list', placeholder: 'group name' },
    { key: 'seen_hashes', label: 'Seen Hashes', type: 'readonly-count' },
  ],
  stealer_log: [
    { key: 'paths', label: 'Paths', type: 'list', placeholder: '/path/to/logs' },
    { key: 'seen_hashes', label: 'Seen Hashes', type: 'readonly-count' },
  ],
};

export default function SourceDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [source, setSource] = useState<Source | null>(null);
  const [channels, setChannels] = useState<TelegramChannel[]>([]);
  const [newChannel, setNewChannel] = useState('');
  const [joinOnAdd, setJoinOnAdd] = useState(true);
  const [adding, setAdding] = useState(false);
  const [removing, setRemoving] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [polling, setPolling] = useState(false);
  const [pollSuccess, setPollSuccess] = useState(false);
  const [configJson, setConfigJson] = useState('');
  const [configDirty, setConfigDirty] = useState(false);
  const [configError, setConfigError] = useState<string | null>(null);
  const [savingConfig, setSavingConfig] = useState(false);
  const [findings, setFindings] = useState<Finding[]>([]);
  const [trend, setTrend] = useState<FindingTrend[]>([]);
  const [showRawConfig, setShowRawConfig] = useState(false);
  const [togglingEnabled, setTogglingEnabled] = useState(false);
  const [errorDismissed, setErrorDismissed] = useState(false);

  const isTelegram = source?.source_type === 'telegram';

  const parsedConfig: Record<string, unknown> = (() => {
    try { return JSON.parse(configJson); } catch { return {}; }
  })();

  const updateConfigKey = (key: string, value: unknown) => {
    try {
      const parsed = JSON.parse(configJson);
      parsed[key] = value;
      setConfigJson(JSON.stringify(parsed, null, 2));
      setConfigDirty(true);
      setConfigError(null);
    } catch {
      setConfigError('Cannot update: config JSON is invalid');
    }
  };

  const loadSource = useCallback(async () => {
    if (!id) return;
    const s = await fetchSource(id);
    setSource(s);
    setConfigJson(JSON.stringify(s.config || {}, null, 2));
    setConfigDirty(false);
    setConfigError(null);
  }, [id]);

  useEffect(() => { loadSource(); }, [loadSource]);

  useEffect(() => {
    if (!id || !isTelegram) return;
    fetchChannels(id).then(setChannels);
  }, [id, isTelegram]);

  useEffect(() => {
    if (!id) return;
    fetchSourceFindings(id).then(setFindings);
    fetchSourceFindingsTrend(id).then(setTrend);
  }, [id]);

  const handleAdd = async () => {
    if (!id || !newChannel.trim()) return;
    setAdding(true);
    setError(null);
    try {
      const ch = await addChannel(id, newChannel.trim(), joinOnAdd);
      setChannels(prev => [...prev, ch]);
      setNewChannel('');
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to add channel');
    } finally {
      setAdding(false);
    }
  };

  const handleRemove = async (channel: string) => {
    if (!id) return;
    setRemoving(channel);
    setError(null);
    try {
      await removeChannel(id, channel);
      setChannels(prev => prev.filter(c => c.channel !== channel));
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to remove channel');
    } finally {
      setRemoving(null);
    }
  };

  const handlePoll = async () => {
    if (!id || polling) return;
    setPolling(true);
    setPollSuccess(false);
    setError(null);
    try {
      await triggerPoll(id);
      setPollSuccess(true);
      setTimeout(() => setPollSuccess(false), 3000);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to trigger poll');
    } finally {
      setPolling(false);
    }
  };

  const handleToggleEnabled = async () => {
    if (!id || !source || togglingEnabled) return;
    setTogglingEnabled(true);
    try {
      const updated = await updateSource(id, { enabled: !source.enabled });
      setSource(prev => prev ? { ...prev, ...updated } : prev);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to toggle source');
    } finally {
      setTogglingEnabled(false);
    }
  };

  const handleSaveConfig = async () => {
    if (!id) return;
    setConfigError(null);
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(configJson);
    } catch {
      setConfigError('Invalid JSON');
      return;
    }
    setSavingConfig(true);
    try {
      const updated = await updateSource(id, { config: parsed });
      setSource(updated);
      setConfigDirty(false);
    } catch (e: unknown) {
      setConfigError(e instanceof Error ? e.message : 'Failed to save config');
    } finally {
      setSavingConfig(false);
    }
  };

  if (!source) {
    return <div style={{ color: colors.textDim, padding: 40 }}>Loading...</div>;
  }

  const HealthIcon = healthIcons[source.health] || Wifi;
  const hColor = healthColor(source.health);
  const hBg = healthBg(source.health);
  const fields = sourceConfigFields[source.source_type] || [];

  const inputStyle: CSSProperties = {
    flex: 1,
    background: colors.bgSurface,
    border: `1px solid ${colors.border}`,
    borderRadius: 6,
    padding: '8px 12px',
    color: colors.text,
    fontSize: 13,
    fontFamily: font.mono,
    outline: 'none',
  };

  const btnPrimary: CSSProperties = {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 6,
    padding: '8px 16px',
    background: colors.accent,
    color: '#fff',
    border: 'none',
    borderRadius: 6,
    fontSize: 13,
    fontWeight: 600,
    cursor: 'pointer',
  };

  const btnDanger: CSSProperties = {
    display: 'inline-flex',
    alignItems: 'center',
    padding: '6px 8px',
    background: 'transparent',
    color: colors.textMuted,
    border: `1px solid ${colors.border}`,
    borderRadius: 6,
    cursor: 'pointer',
    transition: 'color 0.15s, border-color 0.15s',
  };

  const btnOutline: CSSProperties = {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 6,
    padding: '8px 16px',
    background: 'transparent',
    color: colors.text,
    border: `1px solid ${colors.border}`,
    borderRadius: 6,
    fontSize: 13,
    fontWeight: 500,
    cursor: 'pointer',
    transition: 'border-color 0.15s, background 0.15s',
  };

  const sectionTitle: CSSProperties = {
    fontSize: 16,
    fontWeight: 600,
    marginBottom: 16,
    display: 'flex',
    alignItems: 'center',
    gap: 8,
  };

  return (
    <div>
      {/* Back link */}
      <button
        onClick={() => navigate('/sources')}
        style={{
          display: 'inline-flex', alignItems: 'center', gap: 6,
          background: 'none', border: 'none', color: colors.accent,
          cursor: 'pointer', fontSize: 13, marginBottom: 20, padding: 0,
        }}
      >
        <ArrowLeft size={14} /> Sources
      </button>

      {/* Header with toggle and poll button */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 28 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          <div>
            <h1 style={{ fontSize: 24, fontWeight: 700, marginBottom: 4 }}>{source.name}</h1>
            <div style={{ fontSize: 13, color: colors.textMuted, fontFamily: font.mono }}>
              {sourceLabels[source.source_type] || source.source_type}
            </div>
          </div>
          <span style={makeBadge(hColor, hBg)}>
            <HealthIcon size={12} />
            {source.health}
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {/* Enable/disable toggle */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 12, color: source.enabled ? colors.healthy : colors.textMuted, fontWeight: 500 }}>
              {source.enabled ? 'Enabled' : 'Disabled'}
            </span>
            <button
              onClick={handleToggleEnabled}
              disabled={togglingEnabled}
              title={source.enabled ? 'Disable source' : 'Enable source'}
              style={{
                position: 'relative',
                width: 44,
                height: 24,
                borderRadius: 12,
                border: 'none',
                background: source.enabled ? colors.healthy : colors.border,
                cursor: togglingEnabled ? 'wait' : 'pointer',
                transition: 'background 0.2s',
                padding: 0,
                opacity: togglingEnabled ? 0.6 : 1,
              }}
            >
              <div style={{
                position: 'absolute',
                top: 3,
                left: source.enabled ? 23 : 3,
                width: 18,
                height: 18,
                borderRadius: '50%',
                background: '#fff',
                transition: 'left 0.2s',
              }} />
            </button>
          </div>
          <button
            onClick={handlePoll}
            disabled={polling || !source.enabled}
            style={{
              ...btnPrimary,
              opacity: (polling || !source.enabled) ? 0.6 : 1,
              background: pollSuccess ? colors.healthy : colors.accent,
            }}
          >
            {polling ? <Loader size={14} /> : pollSuccess ? <Check size={14} /> : <Play size={14} />}
            {polling ? 'Polling...' : pollSuccess ? 'Dispatched' : 'Poll Now'}
          </button>
        </div>
      </div>

      {/* Degraded/error status banner */}
      {source.last_error && !errorDismissed && (
        <div style={{
          ...card,
          background: source.health === 'degraded' ? colors.degradedBg : colors.criticalBg,
          borderColor: source.health === 'degraded' ? 'rgba(234, 179, 8, 0.3)' : 'rgba(239, 68, 68, 0.3)',
          marginBottom: 16,
          padding: '14px 20px',
        }}>
          <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 12 }}>
            <div style={{ flex: 1 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                <AlertTriangle size={16} color={source.health === 'degraded' ? colors.degraded : colors.critical} />
                <span style={{ fontSize: 14, fontWeight: 600, color: source.health === 'degraded' ? colors.degraded : colors.critical }}>
                  Source {source.health === 'degraded' ? 'Degraded' : 'Error'}
                </span>
              </div>
              <div style={{ fontSize: 13, color: colors.text, fontFamily: font.mono, marginBottom: 10, lineHeight: 1.5 }}>
                {source.last_error}
              </div>
              <div style={{ display: 'flex', gap: 20, fontSize: 12, color: colors.textMuted }}>
                <span>Last polled: {source.last_polled_at ? new Date(source.last_polled_at).toLocaleString() : 'Never'}</span>
                <span>Poll interval: {Math.floor(source.poll_interval_seconds / 60)}m</span>
              </div>
            </div>
            <button
              onClick={() => setErrorDismissed(true)}
              style={{ background: 'none', border: 'none', color: colors.textMuted, cursor: 'pointer', padding: 4, flexShrink: 0 }}
              title="Dismiss"
            >
              <X size={16} />
            </button>
          </div>
        </div>
      )}

      {/* Error banner */}
      {error && (
        <div style={{
          ...card,
          background: colors.criticalBg,
          borderColor: 'rgba(239, 68, 68, 0.3)',
          color: colors.critical,
          fontSize: 13,
          marginBottom: 16,
          padding: '10px 16px',
        }}>
          {error}
        </div>
      )}

      {/* Source info card */}
      <div style={{ ...card, marginBottom: 24 }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 20 }}>
          <InfoField label="Source ID" value={source.id} mono />
          <InfoField label="Type" value={sourceLabels[source.source_type] || source.source_type} />
          <InfoField label="Status" value={source.enabled ? 'Enabled' : 'Disabled'} color={source.enabled ? colors.healthy : colors.textMuted} />
          <InfoField label="Last Poll" value={source.last_polled_at ? new Date(source.last_polled_at).toLocaleString() : 'Never'} />
          <InfoField label="Findings" value={String(source.finding_count)} />
          <InfoField label="Poll Interval" value={`${Math.floor(source.poll_interval_seconds / 60)}m`} />
        </div>
      </div>

      {/* Two-column layout: left (config + channels) / right (chart + findings) */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, alignItems: 'start' }}>
        {/* Left column */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
          {/* Config editor */}
          <div style={card}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
              <h2 style={{ ...sectionTitle, marginBottom: 0 }}>
                <Settings size={16} color={colors.accent} />
                Configuration
              </h2>
              <button
                onClick={() => setShowRawConfig(!showRawConfig)}
                style={{
                  display: 'inline-flex', alignItems: 'center', gap: 4,
                  background: 'none', border: 'none', color: colors.textMuted,
                  cursor: 'pointer', fontSize: 11, padding: '4px 8px',
                }}
              >
                <Code size={12} />
                {showRawConfig ? 'Structured' : 'Raw JSON'}
              </button>
            </div>

            {showRawConfig ? (
              <>
                <textarea
                  value={configJson}
                  onChange={e => { setConfigJson(e.target.value); setConfigDirty(true); setConfigError(null); }}
                  style={{
                    width: '100%',
                    minHeight: 180,
                    background: colors.bgSurface,
                    border: `1px solid ${configError ? colors.critical : colors.border}`,
                    borderRadius: 6,
                    padding: 12,
                    color: colors.text,
                    fontSize: 12,
                    fontFamily: font.mono,
                    lineHeight: 1.5,
                    outline: 'none',
                    resize: 'vertical',
                    boxSizing: 'border-box',
                  }}
                />
              </>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                {fields.length === 0 ? (
                  <div style={{ fontSize: 12, color: colors.textMuted }}>
                    No structured fields defined for this source type. Use Raw JSON to edit.
                  </div>
                ) : (
                  fields.map(field => (
                    <ConfigField
                      key={field.key}
                      field={field}
                      config={parsedConfig}
                      onUpdate={updateConfigKey}
                      inputStyle={inputStyle}
                    />
                  ))
                )}
              </div>
            )}

            {configError && (
              <div style={{ color: colors.critical, fontSize: 12, marginTop: 6 }}>{configError}</div>
            )}
            {configDirty && (
              <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: 10 }}>
                <button
                  onClick={handleSaveConfig}
                  disabled={savingConfig}
                  style={{ ...btnPrimary, opacity: savingConfig ? 0.6 : 1 }}
                >
                  {savingConfig ? <Loader size={14} /> : <Save size={14} />}
                  Save Config
                </button>
              </div>
            )}
          </div>

          {/* Channel management (Telegram only) */}
          {isTelegram && (
            <div style={card}>
              <h2 style={sectionTitle}>
                <MessageSquare size={16} color={colors.accent} />
                Monitored Channels ({channels.length})
              </h2>

              {/* Add channel form */}
              <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 16 }}>
                <input
                  style={inputStyle}
                  value={newChannel}
                  onChange={e => setNewChannel(e.target.value)}
                  placeholder="@channel or https://t.me/+invite"
                  onKeyDown={e => e.key === 'Enter' && handleAdd()}
                  disabled={adding}
                />
                <label style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 12, color: colors.textDim, whiteSpace: 'nowrap', cursor: 'pointer' }}>
                  <input
                    type="checkbox"
                    checked={joinOnAdd}
                    onChange={e => setJoinOnAdd(e.target.checked)}
                    style={{ accentColor: colors.accent }}
                  />
                  Join
                </label>
                <button
                  style={{ ...btnPrimary, opacity: adding ? 0.6 : 1 }}
                  onClick={handleAdd}
                  disabled={adding || !newChannel.trim()}
                >
                  {adding ? <Loader size={14} /> : <Plus size={14} />}
                  Add
                </button>
              </div>

              {/* Channel list */}
              {channels.length === 0 ? (
                <div style={{ textAlign: 'center', color: colors.textMuted, padding: 20, fontSize: 13 }}>
                  No channels monitored. Add a channel to start collecting messages.
                </div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {channels.map(ch => (
                    <div
                      key={ch.channel}
                      style={{
                        background: colors.bgSurface,
                        border: `1px solid ${colors.border}`,
                        borderRadius: 6,
                        padding: '10px 14px',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                      }}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <Hash size={13} color={colors.accent} />
                        <span style={{ fontFamily: font.mono, fontSize: 13 }}>{ch.channel}</span>
                        {ch.last_message_id != null && (
                          <span style={{ fontSize: 11, color: colors.textMuted }}>
                            HWM: {ch.last_message_id}
                          </span>
                        )}
                      </div>
                      <button
                        style={{
                          ...btnDanger,
                          opacity: removing === ch.channel ? 0.5 : 1,
                        }}
                        onClick={() => handleRemove(ch.channel)}
                        disabled={removing === ch.channel}
                        title="Remove channel"
                        onMouseEnter={e => {
                          (e.currentTarget as HTMLButtonElement).style.color = colors.critical;
                          (e.currentTarget as HTMLButtonElement).style.borderColor = colors.critical;
                        }}
                        onMouseLeave={e => {
                          (e.currentTarget as HTMLButtonElement).style.color = colors.textMuted;
                          (e.currentTarget as HTMLButtonElement).style.borderColor = colors.border;
                        }}
                      >
                        {removing === ch.channel ? <Loader size={14} /> : <Trash2 size={14} />}
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>

        {/* Right column */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
          {/* Findings trend chart */}
          <div style={card}>
            <h2 style={sectionTitle}>
              <BarChart3 size={16} color={colors.accent} />
              Findings Trend (14 days)
            </h2>
            {trend.length > 0 ? (
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={trend}>
                  <XAxis
                    dataKey="date"
                    tick={{ fill: colors.textMuted, fontSize: 10 }}
                    tickFormatter={v => v.slice(5)}
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{ fill: colors.textMuted, fontSize: 10 }}
                    axisLine={false}
                    tickLine={false}
                    width={28}
                    allowDecimals={false}
                  />
                  <Tooltip
                    contentStyle={{
                      background: colors.bgSurface,
                      border: `1px solid ${colors.border}`,
                      borderRadius: 6,
                      fontSize: 12,
                      color: colors.text,
                    }}
                    cursor={{ fill: 'rgba(99,102,241,0.08)' }}
                  />
                  <Bar dataKey="count" fill={colors.accent} radius={[3, 3, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ textAlign: 'center', color: colors.textMuted, padding: 40, fontSize: 13 }}>
                No findings data available
              </div>
            )}
          </div>

          {/* Recent findings for this source */}
          <div style={card}>
            <h2 style={sectionTitle}>
              <Activity size={16} color={colors.accent} />
              Recent Findings ({findings.length})
            </h2>
            {findings.length === 0 ? (
              <div style={{ textAlign: 'center', color: colors.textMuted, padding: 20, fontSize: 13 }}>
                No findings from this source yet
              </div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {findings.slice(0, 10).map(f => (
                  <div
                    key={f.id}
                    onClick={() => navigate(`/findings/${f.id}`)}
                    style={{
                      background: colors.bgSurface,
                      border: `1px solid ${colors.border}`,
                      borderRadius: 6,
                      padding: '10px 14px',
                      cursor: 'pointer',
                      transition: 'background 0.15s',
                    }}
                    onMouseEnter={e => { e.currentTarget.style.background = colors.bgHover; }}
                    onMouseLeave={e => { e.currentTarget.style.background = colors.bgSurface; }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                      <span style={{
                        display: 'inline-block',
                        width: 8,
                        height: 8,
                        borderRadius: '50%',
                        background: severityColor(f.severity),
                        flexShrink: 0,
                      }} />
                      <span style={{ fontSize: 13, fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {f.title}
                      </span>
                    </div>
                    <div style={{ display: 'flex', gap: 12, fontSize: 11, color: colors.textMuted }}>
                      <span style={{
                        ...makeBadge(severityColor(f.severity), severityBg(f.severity)),
                        fontSize: 10,
                        padding: '1px 6px',
                      }}>
                        {f.severity}
                      </span>
                      <span>{f.institution_name}</span>
                      <span>{timeAgo(f.discovered_at)}</span>
                    </div>
                  </div>
                ))}
                {findings.length > 10 && (
                  <button
                    onClick={() => navigate(`/findings?source_type=${source.source_type}`)}
                    style={{ ...btnOutline, justifyContent: 'center', marginTop: 4 }}
                  >
                    View all {findings.length} findings
                  </button>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function ConfigField({
  field, config, onUpdate, inputStyle,
}: {
  field: ConfigFieldDef;
  config: Record<string, unknown>;
  onUpdate: (key: string, value: unknown) => void;
  inputStyle: CSSProperties;
}) {
  const [newItem, setNewItem] = useState('');
  const [newKey, setNewKey] = useState('');
  const [newValue, setNewValue] = useState('');
  const [showMasked, setShowMasked] = useState<Record<string, boolean>>({});

  const labelStyle: CSSProperties = {
    fontSize: 11, fontWeight: 600, color: colors.textMuted,
    textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: 8,
  };

  const itemStyle: CSSProperties = {
    background: colors.bgSurface,
    border: `1px solid ${colors.border}`,
    borderRadius: 6,
    padding: '8px 12px',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    fontSize: 13,
    fontFamily: font.mono,
  };

  if (field.type === 'list') {
    const items = Array.isArray(config[field.key]) ? (config[field.key] as string[]) : [];
    return (
      <div>
        <div style={labelStyle}>{field.label} ({items.length})</div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {items.map((item, idx) => (
            <div key={idx} style={itemStyle}>
              <span>{item}</span>
              <button
                onClick={() => onUpdate(field.key, items.filter((_, i) => i !== idx))}
                style={{
                  background: 'none', border: 'none', color: colors.textMuted,
                  cursor: 'pointer', padding: 4,
                }}
                title="Remove"
              >
                <Trash2 size={13} />
              </button>
            </div>
          ))}
        </div>
        <div style={{ display: 'flex', gap: 8, marginTop: 8 }}>
          <input
            style={inputStyle}
            value={newItem}
            onChange={e => setNewItem(e.target.value)}
            placeholder={field.placeholder || 'Add item...'}
            onKeyDown={e => {
              if (e.key === 'Enter' && newItem.trim()) {
                onUpdate(field.key, [...items, newItem.trim()]);
                setNewItem('');
              }
            }}
          />
          <button
            onClick={() => {
              if (newItem.trim()) {
                onUpdate(field.key, [...items, newItem.trim()]);
                setNewItem('');
              }
            }}
            disabled={!newItem.trim()}
            style={{
              display: 'inline-flex', alignItems: 'center', gap: 4,
              padding: '8px 12px', background: colors.accent, color: '#fff',
              border: 'none', borderRadius: 6, fontSize: 12, cursor: 'pointer',
              opacity: newItem.trim() ? 1 : 0.5,
            }}
          >
            <Plus size={13} /> Add
          </button>
        </div>
      </div>
    );
  }

  if (field.type === 'readonly-count') {
    const val = config[field.key];
    const count = Array.isArray(val) ? val.length : (val && typeof val === 'object' ? Object.keys(val).length : 0);
    return (
      <div>
        <div style={labelStyle}>{field.label}</div>
        <div style={{ ...itemStyle, color: colors.textMuted }}>
          {count} tracked entries
        </div>
      </div>
    );
  }

  if (field.type === 'readonly-map') {
    const val = config[field.key];
    const entries = val && typeof val === 'object' && !Array.isArray(val)
      ? Object.entries(val as Record<string, unknown>)
      : [];
    return (
      <div>
        <div style={labelStyle}>{field.label} ({entries.length})</div>
        {entries.length === 0 ? (
          <div style={{ ...itemStyle, color: colors.textMuted }}>No entries</div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {entries.map(([k, v]) => (
              <div key={k} style={itemStyle}>
                <span style={{ color: colors.text }}>{k}</span>
                <span style={{ color: colors.textMuted, fontSize: 11 }}>{String(v)}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  if (field.type === 'key-value') {
    const val = config[field.key];
    const entries = val && typeof val === 'object' && !Array.isArray(val)
      ? Object.entries(val as Record<string, string>)
      : [];
    return (
      <div>
        <div style={labelStyle}>{field.label} ({entries.length})</div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {entries.map(([k, v]) => (
            <div key={k} style={itemStyle}>
              <span style={{ color: colors.text }}>{k}</span>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ color: colors.textMuted, fontSize: 12, fontFamily: font.mono }}>
                  {field.masked && !showMasked[k] ? '••••••••' : String(v)}
                </span>
                {field.masked && (
                  <button
                    onClick={() => setShowMasked(prev => ({ ...prev, [k]: !prev[k] }))}
                    style={{ background: 'none', border: 'none', color: colors.textMuted, cursor: 'pointer', padding: 2 }}
                  >
                    {showMasked[k] ? <EyeOff size={12} /> : <Eye size={12} />}
                  </button>
                )}
                <button
                  onClick={() => {
                    const updated = { ...(val as Record<string, string>) };
                    delete updated[k];
                    onUpdate(field.key, updated);
                  }}
                  style={{ background: 'none', border: 'none', color: colors.textMuted, cursor: 'pointer', padding: 2 }}
                >
                  <Trash2 size={13} />
                </button>
              </div>
            </div>
          ))}
        </div>
        <div style={{ display: 'flex', gap: 8, marginTop: 8 }}>
          <input
            style={{ ...inputStyle, flex: 1 }}
            value={newKey}
            onChange={e => setNewKey(e.target.value)}
            placeholder="Key"
          />
          <input
            style={{ ...inputStyle, flex: 2 }}
            value={newValue}
            onChange={e => setNewValue(e.target.value)}
            placeholder="Value"
            type={field.masked ? 'password' : 'text'}
            onKeyDown={e => {
              if (e.key === 'Enter' && newKey.trim() && newValue.trim()) {
                const updated = { ...(val as Record<string, string> || {}), [newKey.trim()]: newValue.trim() };
                onUpdate(field.key, updated);
                setNewKey('');
                setNewValue('');
              }
            }}
          />
          <button
            onClick={() => {
              if (newKey.trim() && newValue.trim()) {
                const updated = { ...(val as Record<string, string> || {}), [newKey.trim()]: newValue.trim() };
                onUpdate(field.key, updated);
                setNewKey('');
                setNewValue('');
              }
            }}
            disabled={!newKey.trim() || !newValue.trim()}
            style={{
              display: 'inline-flex', alignItems: 'center', gap: 4,
              padding: '8px 12px', background: colors.accent, color: '#fff',
              border: 'none', borderRadius: 6, fontSize: 12, cursor: 'pointer',
              opacity: newKey.trim() && newValue.trim() ? 1 : 0.5,
            }}
          >
            <Plus size={13} /> Add
          </button>
        </div>
      </div>
    );
  }

  return null;
}

function InfoField({ label, value, mono, color }: { label: string; value: string; mono?: boolean; color?: string }) {
  return (
    <div>
      <div style={{ fontSize: 10, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: 4 }}>
        {label}
      </div>
      <div style={{ fontSize: 13, fontWeight: 500, fontFamily: mono ? font.mono : undefined, color: color || colors.text }}>
        {value}
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
