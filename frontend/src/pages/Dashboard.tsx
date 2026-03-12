import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { ShieldAlert, AlertTriangle, Eye, Building2, Radio } from 'lucide-react';
import { fetchDashboardStats, fetchFindings } from '../api';
import { colors, card, severityColor, severityBg, font } from '../theme';
import SeverityBadge from '../components/SeverityBadge';
import StatusBadge from '../components/StatusBadge';
import type { DashboardStats, Finding, Severity } from '../types';
import type { CSSProperties } from 'react';

const statCard = (accent: string): CSSProperties => ({
  ...card,
  display: 'flex',
  alignItems: 'center',
  gap: 16,
  flex: '1 1 0',
  minWidth: 200,
});

const iconBox = (bg: string): CSSProperties => ({
  width: 48,
  height: 48,
  borderRadius: 10,
  background: bg,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  flexShrink: 0,
});

const sevOrder: Severity[] = ['critical', 'high', 'medium', 'low', 'info'];

export default function Dashboard() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [recent, setRecent] = useState<Finding[]>([]);
  const navigate = useNavigate();

  useEffect(() => {
    fetchDashboardStats().then(setStats);
    fetchFindings().then(f => setRecent(f.slice(0, 8)));
  }, []);

  if (!stats) return <div style={{ color: colors.textDim, padding: 40 }}>Loading...</div>;

  const pieData = sevOrder.map(s => ({ name: s, value: stats.findings_by_severity[s] }));

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 700, marginBottom: 4 }}>Dashboard</h1>
      <p style={{ color: colors.textDim, fontSize: 14, marginBottom: 28 }}>Dark web threat intelligence overview</p>

      {/* Stat cards */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 28 }}>
        <div style={statCard(colors.critical)}>
          <div style={iconBox(colors.criticalBg)}><ShieldAlert size={22} color={colors.critical} /></div>
          <div>
            <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1 }}>{stats.findings_by_severity.critical}</div>
            <div style={{ fontSize: 12, color: colors.textDim, marginTop: 2 }}>Critical Findings</div>
          </div>
        </div>
        <div style={statCard(colors.high)}>
          <div style={iconBox(colors.highBg)}><AlertTriangle size={22} color={colors.high} /></div>
          <div>
            <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1 }}>{stats.new_today}</div>
            <div style={{ fontSize: 12, color: colors.textDim, marginTop: 2 }}>New Today</div>
          </div>
        </div>
        <div style={statCard(colors.accent)}>
          <div style={iconBox('rgba(99,102,241,0.12)')}><Eye size={22} color={colors.accent} /></div>
          <div>
            <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1 }}>{stats.total_findings}</div>
            <div style={{ fontSize: 12, color: colors.textDim, marginTop: 2 }}>Total Findings</div>
          </div>
        </div>
        <div style={statCard(colors.healthy)}>
          <div style={iconBox(colors.healthyBg)}><Building2 size={22} color={colors.healthy} /></div>
          <div>
            <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1 }}>{stats.monitored_institutions}</div>
            <div style={{ fontSize: 12, color: colors.textDim, marginTop: 2 }}>Institutions</div>
          </div>
        </div>
        <div style={statCard(colors.medium)}>
          <div style={iconBox(colors.mediumBg)}><Radio size={22} color={colors.medium} /></div>
          <div>
            <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1 }}>{stats.active_sources}</div>
            <div style={{ fontSize: 12, color: colors.textDim, marginTop: 2 }}>Active Sources</div>
          </div>
        </div>
      </div>

      {/* Charts row */}
      <div style={{ display: 'flex', gap: 16, marginBottom: 28, flexWrap: 'wrap' }}>
        <div style={{ ...card, flex: '2 1 400px', minWidth: 0 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 16, color: colors.textDim }}>Findings Trend (14 days)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={stats.findings_trend}>
              <XAxis dataKey="date" tick={{ fill: colors.textMuted, fontSize: 11 }} tickFormatter={v => v.slice(5)} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: colors.textMuted, fontSize: 11 }} axisLine={false} tickLine={false} width={30} />
              <Tooltip
                contentStyle={{ background: colors.bgSurface, border: `1px solid ${colors.border}`, borderRadius: 6, fontSize: 12, color: colors.text }}
                cursor={{ fill: 'rgba(99,102,241,0.08)' }}
              />
              <Bar dataKey="count" fill={colors.accent} radius={[4, 4, 0, 0]} cursor="pointer" onClick={(data: any) => {
                if (data?.date) navigate(`/findings?date=${data.date}`);
              }} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div style={{ ...card, flex: '1 1 240px', minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8, color: colors.textDim, alignSelf: 'flex-start' }}>Severity Distribution</h3>
          <ResponsiveContainer width="100%" height={180}>
            <PieChart>
              <Pie data={pieData} cx="50%" cy="50%" innerRadius={45} outerRadius={75} dataKey="value" stroke="none" cursor="pointer" onClick={(_: any, index: number) => {
                const sev = pieData[index]?.name;
                if (sev) navigate(`/findings?severity=${sev}`);
              }}>
                {pieData.map(d => <Cell key={d.name} fill={severityColor(d.name)} />)}
              </Pie>
              <Tooltip
                contentStyle={{ background: colors.bgSurface, border: `1px solid ${colors.border}`, borderRadius: 6, fontSize: 12, color: colors.text }}
              />
            </PieChart>
          </ResponsiveContainer>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px 14px', justifyContent: 'center', marginTop: 4 }}>
            {pieData.map(d => (
              <div key={d.name} style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 11, color: colors.textDim }}>
                <span style={{ width: 8, height: 8, borderRadius: 2, background: severityColor(d.name) }} />
                {d.name} ({d.value})
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Recent findings */}
      <div style={card}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: colors.textDim }}>Recent Findings</h3>
          <button
            onClick={() => navigate('/findings')}
            style={{
              background: 'none', border: `1px solid ${colors.border}`, borderRadius: 6,
              color: colors.textDim, padding: '6px 14px', fontSize: 12, cursor: 'pointer',
            }}
          >
            View All
          </button>
        </div>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
              {['Severity', 'Title', 'Institution', 'Source', 'Status', 'Discovered'].map(h => (
                <th key={h} style={{ textAlign: 'left', padding: '8px 12px', color: colors.textMuted, fontWeight: 500, fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.05em' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {recent.map(f => (
              <tr key={f.id} style={{ borderBottom: `1px solid ${colors.border}` }}
                onMouseEnter={e => (e.currentTarget.style.background = colors.bgHover)}
                onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
              >
                <td style={{ padding: '10px 12px' }}><SeverityBadge severity={f.severity} /></td>
                <td style={{ padding: '10px 12px', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{f.title}</td>
                <td style={{ padding: '10px 12px', color: colors.textDim }}>{f.institution_name}</td>
                <td style={{ padding: '10px 12px', color: colors.textDim, fontFamily: font.mono, fontSize: 11 }}>{f.source_type}</td>
                <td style={{ padding: '10px 12px' }}><StatusBadge status={f.status} /></td>
                <td style={{ padding: '10px 12px', color: colors.textMuted, fontSize: 12 }}>{timeAgo(f.discovered_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
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
