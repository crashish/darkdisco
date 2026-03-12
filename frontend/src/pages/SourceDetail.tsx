import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { fetchSource, fetchChannels, addChannel, removeChannel } from '../api';
import { colors, card, font, badge as makeBadge, healthColor, healthBg } from '../theme';
import type { Source, TelegramChannel } from '../types';
import { ArrowLeft, Plus, Trash2, MessageSquare, Hash, Wifi, WifiOff, AlertTriangle, Loader } from 'lucide-react';
import type { CSSProperties } from 'react';

const sourceLabels: Record<string, string> = {
  tor_forum: 'Tor Forum',
  paste_site: 'Paste Site',
  telegram: 'Telegram',
  breach_db: 'Breach Database',
  ransomware_blog: 'Ransomware Blog',
};

const healthIcons: Record<string, typeof Wifi> = {
  healthy: Wifi,
  degraded: AlertTriangle,
  offline: WifiOff,
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

  const isTelegram = source?.source_type === 'telegram';

  useEffect(() => {
    if (!id) return;
    fetchSource(id).then(setSource);
  }, [id]);

  useEffect(() => {
    if (!id || !isTelegram) return;
    fetchChannels(id).then(setChannels);
  }, [id, isTelegram]);

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

  if (!source) {
    return <div style={{ color: colors.textDim, padding: 40 }}>Loading...</div>;
  }

  const HealthIcon = healthIcons[source.health] || Wifi;
  const hColor = healthColor(source.health);
  const hBg = healthBg(source.health);

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

      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 28 }}>
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

      {/* Channel management (Telegram only) */}
      {isTelegram && (
        <div>
          <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 16, display: 'flex', alignItems: 'center', gap: 8 }}>
            <MessageSquare size={18} color={colors.accent} />
            Channels ({channels.length})
          </h2>

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

          {/* Add channel form */}
          <div style={{ ...card, marginBottom: 16, padding: '16px 20px' }}>
            <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
              <input
                style={inputStyle}
                value={newChannel}
                onChange={e => setNewChannel(e.target.value)}
                placeholder="@channel_username or https://t.me/+invite_hash"
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
          </div>

          {/* Channel list */}
          {channels.length === 0 ? (
            <div style={{ ...card, textAlign: 'center', color: colors.textMuted, padding: 40 }}>
              No channels configured. Add a Telegram channel above.
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {channels.map(ch => (
                <div
                  key={ch.channel}
                  style={{
                    ...card,
                    padding: '12px 20px',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <Hash size={14} color={colors.accent} />
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
