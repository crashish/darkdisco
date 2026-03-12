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

const configHints: Record<string, string[]> = {
  telegram: ['channels', 'last_message_ids'],
  paste_site: ['paste_sites', 'seen_hashes'],
  tor_forum: ['forums', 'last_thread_ids'],
  forum: ['forums', 'last_thread_ids'],
  breach_db: ['services', 'api_keys'],
  ransomware_blog: ['groups', 'seen_hashes'],
  stealer_log: ['paths', 'seen_hashes'],
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

  const isTelegram = source?.source_type === 'telegram';

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
  const hints = configHints[source.source_type] || [];

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

      {/* Header with poll button */}
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
        <button
          onClick={handlePoll}
          disabled={polling || source.health === 'offline'}
          style={{
            ...btnPrimary,
            opacity: polling ? 0.6 : 1,
            background: pollSuccess ? colors.healthy : colors.accent,
          }}
        >
          {polling ? <Loader size={14} /> : pollSuccess ? <Check size={14} /> : <Play size={14} />}
          {polling ? 'Polling...' : pollSuccess ? 'Dispatched' : 'Poll Now'}
        </button>
      </div>

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
          <InfoField label="Last Poll" value={source.last_poll ? new Date(source.last_poll).toLocaleString() : 'Never'} />
          <InfoField label="Findings" value={String(source.finding_count)} />
          <InfoField label="Poll Interval" value={`${Math.floor(source.avg_poll_seconds / 60)}m`} />
        </div>
      </div>

      {/* Two-column layout: left (config + channels) / right (chart + findings) */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, alignItems: 'start' }}>
        {/* Left column */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
          {/* Config editor */}
          <div style={card}>
            <h2 style={sectionTitle}>
              <Settings size={16} color={colors.accent} />
              Configuration
            </h2>
            {hints.length > 0 && (
              <div style={{ fontSize: 11, color: colors.textMuted, marginBottom: 12 }}>
                Expected keys: {hints.map(h => <code key={h} style={{ background: colors.bgSurface, padding: '1px 5px', borderRadius: 3, marginRight: 4 }}>{h}</code>)}
              </div>
            )}
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
                Channels ({channels.length})
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
                <div style={{ textAlign: 'center', color: colors.textMuted, padding: 20 }}>
                  No channels configured.
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

function InfoField({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div>
      <div style={{ fontSize: 10, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: 4 }}>
        {label}
      </div>
      <div style={{ fontSize: 13, fontWeight: 500, fontFamily: mono ? font.mono : undefined, color: colors.text }}>
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
