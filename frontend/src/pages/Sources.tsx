import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { fetchSources } from '../api';
import { colors, card, font, healthColor, healthBg, badge as makeBadge } from '../theme';
import type { Source, SourceHealth } from '../types';
import { Radio, Activity, Clock, Hash, Wifi, WifiOff, AlertTriangle } from 'lucide-react';
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

export default function Sources() {
  const navigate = useNavigate();
  const [sources, setSources] = useState<Source[]>([]);

  useEffect(() => {
    fetchSources().then(setSources);
  }, []);

  const healthCounts = {
    healthy: sources.filter(s => s.health === 'healthy').length,
    degraded: sources.filter(s => s.health === 'degraded').length,
    offline: sources.filter(s => s.health === 'offline').length,
  };

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 700, marginBottom: 4 }}>Sources</h1>
      <p style={{ color: colors.textDim, fontSize: 14, marginBottom: 28 }}>Dark web connector status and health</p>

      {/* Summary pills */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 28 }}>
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

      {/* Source cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(360px, 1fr))', gap: 16 }}>
        {sources.map(src => {
          const Icon = healthIcons[src.health];
          const color = healthColor(src.health);
          const bg = healthBg(src.health);
          const lastPollMinutes = Math.floor((Date.now() - new Date(src.last_poll).getTime()) / 60000);

          return (
            <div key={src.id} style={{ ...card, padding: 0, overflow: 'hidden', cursor: 'pointer' }} onClick={() => navigate(`/sources/${src.id}`)}>
              {/* Header */}
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '16px 20px', borderBottom: `1px solid ${colors.border}` }}>
                <div>
                  <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 4 }}>{src.name}</div>
                  <div style={{ fontSize: 11, color: colors.textMuted, fontFamily: font.mono }}>
                    {sourceLabels[src.source_type] || src.source_type}
                  </div>
                </div>
                <span style={makeBadge(color, bg)}>
                  <Icon size={12} />
                  {src.health}
                </span>
              </div>

              {/* Metrics */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 0 }}>
                <MetricCell
                  icon={<Clock size={14} color={colors.textMuted} />}
                  label="Last Poll"
                  value={lastPollMinutes < 60 ? `${lastPollMinutes}m ago` : `${Math.floor(lastPollMinutes / 60)}h ago`}
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
