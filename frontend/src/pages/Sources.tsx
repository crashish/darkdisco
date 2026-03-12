import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { fetchSources, updateSource } from '../api';
import { colors, card, font, healthColor, healthBg, badge as makeBadge } from '../theme';
import type { Source, SourceHealth } from '../types';
import { Activity, Clock, Hash, Wifi, WifiOff, AlertTriangle, Filter } from 'lucide-react';
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
  telegram_intel: 'Telegram Intel',
  discord: 'Discord',
  breach_db: 'Breach Database',
  ransomware_blog: 'Ransomware Blog',
  stealer_log: 'Stealer Log',
  forum: 'Forum',
  marketplace: 'Marketplace',
  other: 'Other',
};

type FilterMode = 'all' | 'enabled' | 'disabled';

export default function Sources() {
  const navigate = useNavigate();
  const [sources, setSources] = useState<Source[]>([]);
  const [filter, setFilter] = useState<FilterMode>('all');
  const [togglingId, setTogglingId] = useState<string | null>(null);

  useEffect(() => {
    fetchSources().then(setSources);
  }, []);

  const filtered = filter === 'all'
    ? sources
    : filter === 'enabled'
      ? sources.filter(s => s.enabled)
      : sources.filter(s => !s.enabled);

  const healthCounts = {
    healthy: sources.filter(s => s.health === 'healthy').length,
    degraded: sources.filter(s => s.health === 'degraded').length,
    offline: sources.filter(s => s.health === 'offline').length,
  };

  const enabledCount = sources.filter(s => s.enabled).length;
  const disabledCount = sources.filter(s => !s.enabled).length;

  const handleToggle = async (e: React.MouseEvent, src: Source) => {
    e.stopPropagation();
    setTogglingId(src.id);
    try {
      const updated = await updateSource(src.id, { enabled: !src.enabled });
      setSources(prev => prev.map(s => s.id === src.id ? { ...s, ...updated } : s));
    } catch {
      // silently fail, source state unchanged
    } finally {
      setTogglingId(null);
    }
  };

  const filterBtn = (mode: FilterMode): CSSProperties => ({
    padding: '6px 14px',
    fontSize: 12,
    fontWeight: 500,
    border: `1px solid ${filter === mode ? colors.accent : colors.border}`,
    borderRadius: 6,
    background: filter === mode ? 'rgba(99, 102, 241, 0.15)' : 'transparent',
    color: filter === mode ? colors.accent : colors.textDim,
    cursor: 'pointer',
  });

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 700, marginBottom: 4 }}>Sources</h1>
      <p style={{ color: colors.textDim, fontSize: 14, marginBottom: 28 }}>Dark web connector status and health</p>

      {/* Summary pills */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 20 }}>
        {(['healthy', 'degraded', 'offline'] as SourceHealth[]).map(h => {
          const Icon = healthIcons[h];
          const color = healthColor(h);
          const bg = healthBg(h);
          return (
            <div key={h} style={{ ...card, display: 'flex', alignItems: 'center', gap: 12, padding: '14px 20px' }}>
              <div style={{ width: 40, height: 40, borderRadius: 8, background: bg, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <Icon size={20} color={color} />
              </div>
              <div>
                <div style={{ fontSize: 22, fontWeight: 700, lineHeight: 1, color }}>{healthCounts[h]}</div>
                <div style={{ fontSize: 11, color: colors.textMuted, marginTop: 2, textTransform: 'capitalize' }}>{h}</div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Filter bar */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 20 }}>
        <Filter size={14} color={colors.textMuted} />
        <button onClick={() => setFilter('all')} style={filterBtn('all')}>
          All ({sources.length})
        </button>
        <button onClick={() => setFilter('enabled')} style={filterBtn('enabled')}>
          Enabled ({enabledCount})
        </button>
        <button onClick={() => setFilter('disabled')} style={filterBtn('disabled')}>
          Disabled ({disabledCount})
        </button>
      </div>

      {/* Source cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(360px, 1fr))', gap: 16 }}>
        {filtered.map(src => {
          const Icon = healthIcons[src.health];
          const color = healthColor(src.health);
          const bg = healthBg(src.health);
          const lastPollMinutes = src.last_poll
            ? Math.floor((Date.now() - new Date(src.last_poll).getTime()) / 60000)
            : Infinity;
          const isDisabled = !src.enabled;

          return (
            <div
              key={src.id}
              style={{
                ...card,
                padding: 0,
                overflow: 'hidden',
                cursor: 'pointer',
                opacity: isDisabled ? 0.5 : 1,
                transition: 'opacity 0.2s',
              }}
              onClick={() => navigate(`/sources/${src.id}`)}
            >
              {/* Header */}
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '16px 20px', borderBottom: `1px solid ${colors.border}` }}>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 4, color: isDisabled ? colors.textMuted : colors.text }}>{src.name}</div>
                  <div style={{ fontSize: 11, color: colors.textMuted, fontFamily: font.mono }}>
                    {sourceLabels[src.source_type] || src.source_type}
                  </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <ToggleSwitch
                    enabled={src.enabled}
                    loading={togglingId === src.id}
                    onClick={(e) => handleToggle(e, src)}
                  />
                  <span style={makeBadge(color, bg)}>
                    <Icon size={12} />
                    {src.health}
                  </span>
                </div>
              </div>

              {/* Metrics */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 0 }}>
                <MetricCell
                  icon={<Clock size={14} color={colors.textMuted} />}
                  label="Last Poll"
                  value={
                    !src.last_poll ? 'Never'
                    : lastPollMinutes < 60 ? `${lastPollMinutes}m ago`
                    : `${Math.floor(lastPollMinutes / 60)}h ago`
                  }
                  warn={lastPollMinutes > 30}
                />
                <MetricCell
                  icon={<Hash size={14} color={colors.textMuted} />}
                  label="Findings"
                  value={src.finding_count.toLocaleString()}
                />
                <MetricCell
                  icon={<Activity size={14} color={colors.textMuted} />}
                  label="Poll Interval"
                  value={`${Math.floor(src.avg_poll_seconds / 60)}m`}
                />
              </div>

              {/* Health bar */}
              <div style={{ height: 3, background: colors.bgSurface }}>
                <div style={{
                  height: '100%',
                  width: src.health === 'healthy' ? '100%' : src.health === 'degraded' ? '50%' : '0%',
                  background: color,
                  transition: 'width 0.3s',
                }} />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ToggleSwitch({ enabled, loading, onClick }: { enabled: boolean; loading: boolean; onClick: (e: React.MouseEvent) => void }) {
  return (
    <button
      onClick={onClick}
      disabled={loading}
      title={enabled ? 'Disable source' : 'Enable source'}
      style={{
        position: 'relative',
        width: 36,
        height: 20,
        borderRadius: 10,
        border: 'none',
        background: enabled ? colors.healthy : colors.border,
        cursor: loading ? 'wait' : 'pointer',
        transition: 'background 0.2s',
        padding: 0,
        opacity: loading ? 0.6 : 1,
        flexShrink: 0,
      }}
    >
      <div style={{
        position: 'absolute',
        top: 2,
        left: enabled ? 18 : 2,
        width: 16,
        height: 16,
        borderRadius: '50%',
        background: '#fff',
        transition: 'left 0.2s',
      }} />
    </button>
  );
}

function MetricCell({ icon, label, value, warn }: { icon: React.ReactNode; label: string; value: string; warn?: boolean }) {
  return (
    <div style={{ padding: '12px 16px', borderRight: `1px solid ${colors.border}` }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 4 }}>
        {icon}
        <span style={{ fontSize: 10, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{label}</span>
      </div>
      <div style={{ fontSize: 14, fontWeight: 600, color: warn ? colors.medium : colors.text, fontFamily: font.mono }}>
        {value}
      </div>
    </div>
  );
}
