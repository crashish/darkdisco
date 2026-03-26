import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  AreaChart, Area, BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
} from 'recharts';
import {
  Building2, ShieldAlert, AlertTriangle, Users, Radio,
  ArrowLeft, FileText, Clock, TrendingUp,
} from 'lucide-react';
import { fetchThreatSummary, fetchWatchTerms, fetchFindings } from '../api';
import { colors, card, font, severityColor, severityBg, statusColor, statusLabel } from '../theme';
import SeverityBadge from '../components/SeverityBadge';
import StatusBadge from '../components/StatusBadge';
import type { ThreatSummary, WatchTerm, Finding, Severity } from '../types';
import type { CSSProperties } from 'react';

const metricCard: CSSProperties = {
  ...card,
  display: 'flex',
  alignItems: 'center',
  gap: 14,
  flex: '1 1 0',
  minWidth: 180,
};

const iconBox = (bg: string): CSSProperties => ({
  width: 44,
  height: 44,
  borderRadius: 10,
  background: bg,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  flexShrink: 0,
});

const sectionTitle: CSSProperties = {
  fontSize: 14,
  fontWeight: 600,
  color: colors.textDim,
  marginBottom: 16,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
};

const sevOrder: Severity[] = ['critical', 'high', 'medium', 'low', 'info'];

const categoryColors: Record<string, string> = {
  'Card Fraud': '#ef4444',
  'Phishing': '#f97316',
  'Account Takeover': '#eab308',
  'Credential Leaks': '#6366f1',
  'Data Breach': '#ec4899',
  'Other': '#64748b',
};

const periodOptions = [
  { label: '30 days', value: 30 },
  { label: '90 days', value: 90 },
  { label: '180 days', value: 180 },
  { label: '1 year', value: 365 },
];

export default function InstitutionDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [summary, setSummary] = useState<ThreatSummary | null>(null);
  const [watchTerms, setWatchTerms] = useState<WatchTerm[]>([]);
  const [recentFindings, setRecentFindings] = useState<Finding[]>([]);
  const [days, setDays] = useState(90);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    Promise.all([
      fetchThreatSummary(id, days),
      fetchWatchTerms(id),
      fetchFindings({ institution_id: id, page_size: 10 }),
    ]).then(([s, wt, f]) => {
      setSummary(s);
      setWatchTerms(wt);
      setRecentFindings(f.items);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, [id, days]);

  if (loading || !summary) {
    return <div style={{ color: colors.textDim, padding: 40 }}>Loading threat summary...</div>;
  }

  const sevPieData = sevOrder
    .map(s => ({ name: s, value: summary.by_severity[s] || 0 }))
    .filter(d => d.value > 0);

  const statusData = Object.entries(summary.by_status).map(([k, v]) => ({
    name: statusLabel(k),
    value: v,
    key: k,
  }));

  return (
    <div>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 4 }}>
        <button
          onClick={() => navigate('/institutions')}
          style={{
            background: 'none', border: 'none', cursor: 'pointer',
            color: colors.textDim, padding: 4, display: 'flex',
          }}
        >
          <ArrowLeft size={20} />
        </button>
        <Building2 size={24} color={colors.accent} />
        <h1 style={{ fontSize: 24, fontWeight: 700 }}>{summary.institution_name}</h1>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 24, marginLeft: 40 }}>
        <span style={{ color: colors.textDim, fontSize: 14 }}>Threat Summary</span>
        <select
          value={days}
          onChange={e => setDays(Number(e.target.value))}
          style={{
            background: colors.bgSurface, border: `1px solid ${colors.border}`,
            borderRadius: 6, color: colors.text, padding: '4px 8px', fontSize: 12,
            cursor: 'pointer',
          }}
        >
          {periodOptions.map(o => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>
        <span style={{ color: colors.textMuted, fontSize: 12 }}>
          {watchTerms.length} watch term{watchTerms.length !== 1 ? 's' : ''} configured
        </span>
      </div>

      {/* Key Metrics */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 24 }}>
        <div style={metricCard}>
          <div style={iconBox('rgba(99,102,241,0.12)')}>
            <ShieldAlert size={20} color={colors.accent} />
          </div>
          <div>
            <div style={{ fontSize: 26, fontWeight: 700, lineHeight: 1 }}>{summary.total_findings}</div>
            <div style={{ fontSize: 11, color: colors.textDim, marginTop: 2 }}>Total Findings</div>
          </div>
        </div>
        <div style={metricCard}>
          <div style={iconBox(colors.criticalBg)}>
            <AlertTriangle size={20} color={colors.critical} />
          </div>
          <div>
            <div style={{ fontSize: 26, fontWeight: 700, lineHeight: 1, color: summary.confirmed_threats > 0 ? colors.critical : colors.text }}>
              {summary.confirmed_threats}
            </div>
            <div style={{ fontSize: 11, color: colors.textDim, marginTop: 2 }}>Confirmed Threats</div>
          </div>
        </div>
        <div style={metricCard}>
          <div style={iconBox('rgba(234,179,8,0.12)')}>
            <Users size={20} color={colors.medium} />
          </div>
          <div>
            <div style={{ fontSize: 26, fontWeight: 700, lineHeight: 1 }}>{summary.active_threat_actors}</div>
            <div style={{ fontSize: 11, color: colors.textDim, marginTop: 2 }}>Threat Actors</div>
          </div>
        </div>
        <div style={metricCard}>
          <div style={iconBox(colors.healthyBg)}>
            <Radio size={20} color={colors.healthy} />
          </div>
          <div>
            <div style={{ fontSize: 26, fontWeight: 700, lineHeight: 1 }}>{summary.top_source_channels.length}</div>
            <div style={{ fontSize: 11, color: colors.textDim, marginTop: 2 }}>Source Channels</div>
          </div>
        </div>
      </div>

      {/* Charts Row 1: Timeline + Severity */}
      <div style={{ display: 'flex', gap: 16, marginBottom: 24, flexWrap: 'wrap' }}>
        <div style={{ ...card, flex: '2 1 500px', minWidth: 0 }}>
          <h3 style={sectionTitle}>
            <TrendingUp size={16} color={colors.accent} /> Finding Volume Timeline
          </h3>
          <ResponsiveContainer width="100%" height={240}>
            <AreaChart data={summary.findings_timeline}>
              <defs>
                <linearGradient id="timelineGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={colors.accent} stopOpacity={0.3} />
                  <stop offset="95%" stopColor={colors.accent} stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis
                dataKey="date"
                tick={{ fill: colors.textMuted, fontSize: 10 }}
                tickFormatter={v => v.slice(5)}
                axisLine={false}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fill: colors.textMuted, fontSize: 10 }}
                axisLine={false}
                tickLine={false}
                width={30}
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
              />
              <Area
                type="monotone"
                dataKey="count"
                stroke={colors.accent}
                strokeWidth={2}
                fill="url(#timelineGrad)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        <div style={{ ...card, flex: '1 1 280px', minWidth: 0, display: 'flex', flexDirection: 'column' }}>
          <h3 style={sectionTitle}>Severity Distribution</h3>
          <div style={{ flex: 1, minHeight: 180 }}>
            <ResponsiveContainer width="100%" height={180}>
              <PieChart>
                <Pie
                  data={sevPieData}
                  cx="50%" cy="50%"
                  innerRadius={42} outerRadius={72}
                  dataKey="value" stroke="none"
                >
                  {sevPieData.map(d => (
                    <Cell key={d.name} fill={severityColor(d.name)} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{
                    background: colors.bgSurface,
                    border: `1px solid ${colors.border}`,
                    borderRadius: 6,
                    fontSize: 12,
                    color: colors.text,
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px 14px', justifyContent: 'center', marginTop: 8 }}>
            {sevPieData.map(d => (
              <div key={d.name} style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 11, color: colors.textDim }}>
                <span style={{ width: 8, height: 8, borderRadius: 2, background: severityColor(d.name) }} />
                {d.name} ({d.value})
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Charts Row 2: Threat Categories + Source Channels */}
      <div style={{ display: 'flex', gap: 16, marginBottom: 24, flexWrap: 'wrap' }}>
        <div style={{ ...card, flex: '1 1 400px', minWidth: 0 }}>
          <h3 style={sectionTitle}>Threat Category Breakdown</h3>
          {summary.threat_categories.length === 0 ? (
            <div style={{ color: colors.textMuted, fontSize: 13, padding: '20px 0' }}>No categorized findings</div>
          ) : (
            <ResponsiveContainer width="100%" height={Math.max(180, summary.threat_categories.length * 36)}>
              <BarChart
                data={summary.threat_categories}
                layout="vertical"
                margin={{ left: 10, right: 20 }}
              >
                <XAxis type="number" tick={{ fill: colors.textMuted, fontSize: 10 }} axisLine={false} tickLine={false} allowDecimals={false} />
                <YAxis
                  type="category"
                  dataKey="category"
                  tick={{ fill: colors.textDim, fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                  width={120}
                />
                <Tooltip
                  contentStyle={{
                    background: colors.bgSurface,
                    border: `1px solid ${colors.border}`,
                    borderRadius: 6,
                    fontSize: 12,
                    color: colors.text,
                  }}
                />
                <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                  {summary.threat_categories.map((d, i) => (
                    <Cell key={i} fill={categoryColors[d.category] || colors.accent} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        <div style={{ ...card, flex: '1 1 300px', minWidth: 0 }}>
          <h3 style={sectionTitle}>
            <Radio size={16} color={colors.healthy} /> Source Channels
          </h3>
          {summary.top_source_channels.length === 0 ? (
            <div style={{ color: colors.textMuted, fontSize: 13, padding: '20px 0' }}>No source data</div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {summary.top_source_channels.map((src, i) => {
                const maxCount = summary.top_source_channels[0].count;
                const pct = maxCount > 0 ? (src.count / maxCount) * 100 : 0;
                return (
                  <div key={i}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <span style={{ fontSize: 12, color: colors.text, fontFamily: font.mono }}>
                        {src.source_type.replace(/_/g, ' ')}
                      </span>
                      <span style={{ fontSize: 12, fontWeight: 600, color: colors.textDim }}>{src.count}</span>
                    </div>
                    <div style={{
                      height: 6,
                      background: colors.bgSurface,
                      borderRadius: 3,
                      overflow: 'hidden',
                    }}>
                      <div style={{
                        width: `${pct}%`,
                        height: '100%',
                        background: colors.accent,
                        borderRadius: 3,
                        transition: 'width 0.3s',
                      }} />
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {/* Executive Brief */}
      <div style={{ ...card, marginBottom: 24 }}>
        <h3 style={sectionTitle}>
          <FileText size={16} color={colors.accent} /> Executive Brief
        </h3>
        <p style={{
          fontSize: 14,
          lineHeight: 1.7,
          color: colors.text,
          margin: 0,
          whiteSpace: 'pre-wrap',
        }}>
          {summary.executive_brief}
        </p>
      </div>

      {/* Recent Findings */}
      <div style={card}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <h3 style={{ ...sectionTitle, marginBottom: 0 }}>
            <Clock size={16} color={colors.textDim} /> Recent Findings
          </h3>
          <button
            onClick={() => navigate(`/findings?institution_id=${id}`)}
            style={{
              background: 'none', border: `1px solid ${colors.border}`, borderRadius: 6,
              color: colors.textDim, padding: '6px 14px', fontSize: 12, cursor: 'pointer',
            }}
          >
            View All
          </button>
        </div>
        {recentFindings.length === 0 ? (
          <div style={{ color: colors.textMuted, fontSize: 13, padding: '12px 0' }}>No findings recorded</div>
        ) : (
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                {['Severity', 'Title', 'Source', 'Status', 'Discovered'].map(h => (
                  <th key={h} style={{
                    textAlign: 'left', padding: '8px 12px', color: colors.textMuted,
                    fontWeight: 500, fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.05em',
                  }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {recentFindings.map(f => (
                <tr
                  key={f.id}
                  style={{ borderBottom: `1px solid ${colors.border}`, cursor: 'pointer' }}
                  onClick={() => navigate(`/findings/${f.id}`)}
                  onMouseEnter={e => (e.currentTarget.style.background = colors.bgHover)}
                  onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                >
                  <td style={{ padding: '10px 12px' }}><SeverityBadge severity={f.severity} /></td>
                  <td style={{ padding: '10px 12px', maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{f.title}</td>
                  <td style={{ padding: '10px 12px', color: colors.textDim, fontFamily: font.mono, fontSize: 11 }}>{f.source_type}</td>
                  <td style={{ padding: '10px 12px' }}><StatusBadge status={f.status} /></td>
                  <td style={{ padding: '10px 12px', color: colors.textMuted, fontSize: 12 }}>{timeAgo(f.discovered_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
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
