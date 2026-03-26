import { useState, useEffect, type CSSProperties } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
} from 'recharts';
import { fetchDispositionAnalytics, fetchInstitutions } from '../api';
import type { DispositionAnalytics, Institution } from '../types';
import { colors, card, font, statusLabel } from '../theme';

const headerStyle: CSSProperties = {
  display: 'flex', justifyContent: 'space-between', alignItems: 'center',
  marginBottom: 24,
};

const filterRow: CSSProperties = {
  display: 'flex', gap: 12, alignItems: 'center',
};

const selectStyle: CSSProperties = {
  background: colors.bgSurface, color: colors.text, border: `1px solid ${colors.border}`,
  borderRadius: 6, padding: '6px 12px', fontSize: 13, fontFamily: font.sans,
};

const statCard = (accent: string): CSSProperties => ({
  ...card,
  flex: 1,
  minWidth: 160,
  borderTop: `3px solid ${accent}`,
});

const tooltipStyle: CSSProperties = {
  background: colors.bgSurface, border: `1px solid ${colors.border}`,
  borderRadius: 6, fontSize: 12, fontFamily: font.sans,
};

const DISPOSITION_COLORS: Record<string, string> = {
  confirmed: '#22c55e',
  dismissed: '#78716c',
  false_positive: '#64748b',
  escalated: '#ef4444',
  new: '#6366f1',
  reviewing: '#eab308',
  resolved: '#22c55e',
};

const FP_BUCKET_COLORS = ['#22c55e', '#84cc16', '#eab308', '#f97316', '#ef4444'];

export default function Analytics() {
  const navigate = useNavigate();
  const [data, setData] = useState<DispositionAnalytics | null>(null);
  const [institutions, setInstitutions] = useState<Institution[]>([]);
  const [days, setDays] = useState(30);
  const [instFilter, setInstFilter] = useState('');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchInstitutions().then(setInstitutions);
  }, []);

  useEffect(() => {
    setLoading(true);
    fetchDispositionAnalytics({
      days,
      institution_id: instFilter || undefined,
    }).then(d => {
      setData(d);
      setLoading(false);
    });
  }, [days, instFilter]);

  if (loading || !data) {
    return (
      <div style={{ padding: 32, color: colors.textDim, fontFamily: font.sans }}>
        Loading analytics...
      </div>
    );
  }

  const { institution_fp_rates, pattern_effectiveness, analyst_workload, disposition_trends } = data;

  // Summary stats
  const totalFindings = institution_fp_rates.reduce((s, r) => s + r.total_findings, 0);
  const totalFP = institution_fp_rates.reduce((s, r) => s + r.false_positives + r.dismissed, 0);
  const overallFPRate = totalFindings > 0 ? totalFP / totalFindings : 0;

  return (
    <div style={{ fontFamily: font.sans, color: colors.text }}>
      {/* Header */}
      <div style={headerStyle}>
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 700, margin: 0 }}>
            Disposition Analytics
          </h1>
          <p style={{ color: colors.textMuted, fontSize: 13, margin: '4px 0 0' }}>
            Finding disposition rates, pattern effectiveness, and analyst workload
          </p>
        </div>
        <div style={filterRow}>
          <select
            value={days}
            onChange={e => setDays(Number(e.target.value))}
            style={selectStyle}
          >
            <option value={7}>Last 7 days</option>
            <option value={14}>Last 14 days</option>
            <option value={30}>Last 30 days</option>
            <option value={90}>Last 90 days</option>
          </select>
          <select
            value={instFilter}
            onChange={e => setInstFilter(e.target.value)}
            style={{ ...selectStyle, maxWidth: 220 }}
          >
            <option value="">All Institutions</option>
            {institutions.map(i => (
              <option key={i.id} value={i.id}>{i.name}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Summary stat cards */}
      <div style={{ display: 'flex', gap: 16, marginBottom: 24, flexWrap: 'wrap' }}>
        <div style={statCard(colors.accent)}>
          <div style={{ fontSize: 11, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            Total Findings
          </div>
          <div style={{ fontSize: 28, fontWeight: 700, marginTop: 4 }}>
            {totalFindings.toLocaleString()}
          </div>
        </div>
        <div style={statCard(colors.critical)}>
          <div style={{ fontSize: 11, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            FP / Dismissed
          </div>
          <div style={{ fontSize: 28, fontWeight: 700, marginTop: 4, color: totalFP > 0 ? colors.critical : colors.text }}>
            {totalFP.toLocaleString()}
          </div>
          <div style={{ fontSize: 12, color: colors.textMuted, marginTop: 2 }}>
            {(overallFPRate * 100).toFixed(1)}% noise rate
          </div>
        </div>
        <div style={statCard('#eab308')}>
          <div style={{ fontSize: 11, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            Pending Review
          </div>
          <div style={{ fontSize: 28, fontWeight: 700, marginTop: 4 }}>
            {analyst_workload.pending_review}
          </div>
        </div>
        <div style={statCard(colors.healthy)}>
          <div style={{ fontSize: 11, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            Avg Disposition Time
          </div>
          <div style={{ fontSize: 28, fontWeight: 700, marginTop: 4 }}>
            {analyst_workload.avg_disposition_hours != null
              ? `${analyst_workload.avg_disposition_hours}h`
              : '—'}
          </div>
        </div>
        <div style={statCard('#8b5cf6')}>
          <div style={{ fontSize: 11, color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            Suppression Rate
          </div>
          <div style={{ fontSize: 28, fontWeight: 700, marginTop: 4 }}>
            {(pattern_effectiveness.suppression_rate * 100).toFixed(1)}%
          </div>
          <div style={{ fontSize: 12, color: colors.textMuted, marginTop: 2 }}>
            {pattern_effectiveness.total_suppressed.toLocaleString()} of {pattern_effectiveness.total_mentions.toLocaleString()} mentions
          </div>
        </div>
      </div>

      {/* Row 1: Disposition Trends + Disposition Breakdown */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 16, marginBottom: 24 }}>
        {/* Disposition Trends */}
        <div style={card}>
          <h3 style={{ fontSize: 14, fontWeight: 600, margin: '0 0 16px', color: colors.text }}>
            Disposition Trends
          </h3>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={disposition_trends}>
              <XAxis
                dataKey="date"
                tick={{ fill: colors.textMuted, fontSize: 11 }}
                tickFormatter={d => d.slice(5)}
              />
              <YAxis tick={{ fill: colors.textMuted, fontSize: 11 }} />
              <Tooltip contentStyle={tooltipStyle} />
              <Legend
                wrapperStyle={{ fontSize: 11 }}
                formatter={(v: string) => statusLabel(v)}
              />
              <Line type="monotone" dataKey="confirmed" stroke="#22c55e" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="false_positive" stroke="#64748b" strokeWidth={2} dot={false} name="false_positive" />
              <Line type="monotone" dataKey="dismissed" stroke="#78716c" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="escalated" stroke="#ef4444" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="new" stroke="#6366f1" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Disposition Breakdown Pie */}
        <div style={card}>
          <h3 style={{ fontSize: 14, fontWeight: 600, margin: '0 0 16px', color: colors.text }}>
            Status Breakdown
          </h3>
          <ResponsiveContainer width="100%" height={260}>
            <PieChart>
              <Pie
                data={analyst_workload.disposition_breakdown}
                dataKey="count"
                nameKey="status"
                cx="50%"
                cy="50%"
                outerRadius={90}
                innerRadius={50}
                paddingAngle={2}
                label={((props: any) => {  // eslint-disable-line @typescript-eslint/no-explicit-any
                  const s = props.status as string;
                  const c = props.count as number;
                  return c > 0 ? `${statusLabel(s)} (${c})` : '';
                }) as any}
                labelLine={false}
              >
                {analyst_workload.disposition_breakdown.map((entry, i) => (
                  <Cell
                    key={i}
                    fill={DISPOSITION_COLORS[entry.status] || colors.textMuted}
                  />
                ))}
              </Pie>
              <Tooltip contentStyle={tooltipStyle} formatter={(v, name) => [v, statusLabel(String(name))]} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Row 2: FP Rate by Institution + FP Score Distribution */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 16, marginBottom: 24 }}>
        {/* FP Rate by Institution */}
        <div style={card}>
          <h3 style={{ fontSize: 14, fontWeight: 600, margin: '0 0 16px', color: colors.text }}>
            Noise Rate by Institution
          </h3>
          {institution_fp_rates.length === 0 ? (
            <div style={{ color: colors.textMuted, fontSize: 13, padding: 20, textAlign: 'center' }}>
              No findings in selected period
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={Math.max(200, institution_fp_rates.length * 36)}>
              <BarChart
                data={institution_fp_rates.slice(0, 15)}
                layout="vertical"
                margin={{ left: 120 }}
              >
                <XAxis
                  type="number"
                  tick={{ fill: colors.textMuted, fontSize: 11 }}
                  domain={[0, 'dataMax']}
                />
                <YAxis
                  type="category"
                  dataKey="institution_name"
                  tick={{ fill: colors.textDim, fontSize: 11 }}
                  width={110}
                />
                <Tooltip
                  contentStyle={tooltipStyle}
                  formatter={(v, name) => {
                    if (name === 'fp_rate') return [(Number(v) * 100).toFixed(1) + '%', 'Noise Rate'];
                    return [v, name];
                  }}
                />
                <Bar dataKey="confirmed" stackId="a" fill="#22c55e" radius={0} />
                <Bar dataKey="false_positives" stackId="a" fill="#64748b" radius={0} />
                <Bar dataKey="dismissed" stackId="a" fill="#78716c" radius={0} />
                <Legend wrapperStyle={{ fontSize: 11 }} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* FP Score Distribution */}
        <div style={card}>
          <h3 style={{ fontSize: 14, fontWeight: 600, margin: '0 0 16px', color: colors.text }}>
            FP Score Distribution
          </h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={pattern_effectiveness.fp_score_distribution}>
              <XAxis
                dataKey="bucket"
                tick={{ fill: colors.textMuted, fontSize: 10 }}
              />
              <YAxis tick={{ fill: colors.textMuted, fontSize: 11 }} />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                {pattern_effectiveness.fp_score_distribution.map((_, i) => (
                  <Cell key={i} fill={FP_BUCKET_COLORS[i] || colors.accent} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div style={{ fontSize: 11, color: colors.textMuted, marginTop: 8, textAlign: 'center' }}>
            Lower score = likely real threat. Higher = likely false positive.
          </div>
        </div>
      </div>

      {/* Row 3: Analyst Workload + Pattern Stats */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
        {/* Analyst Activity */}
        <div style={card}>
          <h3 style={{ fontSize: 14, fontWeight: 600, margin: '0 0 16px', color: colors.text }}>
            Analyst Activity
          </h3>
          {analyst_workload.by_analyst.length === 0 ? (
            <div style={{ color: colors.textMuted, fontSize: 13, padding: 20, textAlign: 'center' }}>
              No analyst activity in selected period
            </div>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                  {['Analyst', 'Reviewed', 'Pending'].map(h => (
                    <th key={h} style={{
                      textAlign: 'left', padding: '8px 12px',
                      color: colors.textMuted, fontSize: 11, fontWeight: 600,
                      textTransform: 'uppercase', letterSpacing: '0.05em',
                    }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {analyst_workload.by_analyst.map((a, i) => (
                  <tr
                    key={i}
                    style={{ borderBottom: `1px solid ${colors.border}` }}
                    onMouseEnter={e => (e.currentTarget.style.background = colors.bgHover)}
                    onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                  >
                    <td style={{ padding: '8px 12px', fontSize: 13 }}>{a.analyst}</td>
                    <td style={{ padding: '8px 12px', fontSize: 13, fontWeight: 600 }}>{a.reviewed}</td>
                    <td style={{ padding: '8px 12px', fontSize: 13 }}>
                      <span style={{
                        color: a.pending > 10 ? colors.critical : a.pending > 0 ? '#eab308' : colors.healthy,
                        fontWeight: 600,
                      }}>
                        {a.pending}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Pattern / Mention Pipeline Stats */}
        <div style={card}>
          <h3 style={{ fontSize: 14, fontWeight: 600, margin: '0 0 16px', color: colors.text }}>
            Mention Pipeline
          </h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                <span style={{ fontSize: 12, color: colors.textMuted }}>Mentions Collected</span>
                <span style={{ fontSize: 13, fontWeight: 600 }}>
                  {pattern_effectiveness.total_mentions.toLocaleString()}
                </span>
              </div>
              <div style={{
                height: 8, background: colors.bgSurface, borderRadius: 4, overflow: 'hidden',
              }}>
                <div style={{
                  height: '100%', borderRadius: 4,
                  width: '100%', background: colors.accent,
                }} />
              </div>
            </div>
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                <span style={{ fontSize: 12, color: colors.textMuted }}>Promoted to Findings</span>
                <span style={{ fontSize: 13, fontWeight: 600, color: colors.healthy }}>
                  {pattern_effectiveness.total_promoted.toLocaleString()}
                </span>
              </div>
              <div style={{
                height: 8, background: colors.bgSurface, borderRadius: 4, overflow: 'hidden',
              }}>
                <div style={{
                  height: '100%', borderRadius: 4,
                  width: pattern_effectiveness.total_mentions > 0
                    ? `${(pattern_effectiveness.total_promoted / pattern_effectiveness.total_mentions) * 100}%`
                    : '0%',
                  background: colors.healthy,
                }} />
              </div>
            </div>
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                <span style={{ fontSize: 12, color: colors.textMuted }}>Suppressed (Noise Filtered)</span>
                <span style={{ fontSize: 13, fontWeight: 600, color: '#8b5cf6' }}>
                  {pattern_effectiveness.total_suppressed.toLocaleString()}
                </span>
              </div>
              <div style={{
                height: 8, background: colors.bgSurface, borderRadius: 4, overflow: 'hidden',
              }}>
                <div style={{
                  height: '100%', borderRadius: 4,
                  width: pattern_effectiveness.total_mentions > 0
                    ? `${pattern_effectiveness.suppression_rate * 100}%`
                    : '0%',
                  background: '#8b5cf6',
                }} />
              </div>
            </div>
          </div>
          <div style={{
            marginTop: 20, padding: '12px 16px', background: colors.bgSurface,
            borderRadius: 6, fontSize: 12, color: colors.textDim, lineHeight: 1.6,
          }}>
            Suppression includes negative pattern matching and fraud indicator filters.
            Findings that pass through are further scored by the FP detection engine.
          </div>
        </div>
      </div>

      {/* Institution FP Rate Table */}
      {institution_fp_rates.length > 0 && (
        <div style={card}>
          <h3 style={{ fontSize: 14, fontWeight: 600, margin: '0 0 16px', color: colors.text }}>
            Institution Detail
          </h3>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                {['Institution', 'Total', 'Confirmed', 'FP', 'Dismissed', 'Noise Rate'].map(h => (
                  <th key={h} style={{
                    textAlign: h === 'Institution' ? 'left' : 'right',
                    padding: '8px 12px', color: colors.textMuted, fontSize: 11,
                    fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em',
                  }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {institution_fp_rates.map(row => (
                <tr
                  key={row.institution_id}
                  style={{ borderBottom: `1px solid ${colors.border}`, cursor: 'pointer' }}
                  onClick={() => navigate(`/institutions`)}
                  onMouseEnter={e => (e.currentTarget.style.background = colors.bgHover)}
                  onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                >
                  <td style={{ padding: '8px 12px', fontSize: 13 }}>{row.institution_name}</td>
                  <td style={{ padding: '8px 12px', fontSize: 13, textAlign: 'right', fontWeight: 600 }}>
                    {row.total_findings}
                  </td>
                  <td style={{ padding: '8px 12px', fontSize: 13, textAlign: 'right', color: colors.healthy }}>
                    {row.confirmed}
                  </td>
                  <td style={{ padding: '8px 12px', fontSize: 13, textAlign: 'right', color: colors.textMuted }}>
                    {row.false_positives}
                  </td>
                  <td style={{ padding: '8px 12px', fontSize: 13, textAlign: 'right', color: colors.textMuted }}>
                    {row.dismissed}
                  </td>
                  <td style={{ padding: '8px 12px', textAlign: 'right' }}>
                    <span style={{
                      fontSize: 12, fontWeight: 600, padding: '2px 8px', borderRadius: 9999,
                      color: row.fp_rate > 0.5 ? colors.critical : row.fp_rate > 0.25 ? '#eab308' : colors.healthy,
                      background: row.fp_rate > 0.5 ? colors.criticalBg : row.fp_rate > 0.25 ? colors.mediumBg : colors.healthyBg,
                    }}>
                      {(row.fp_rate * 100).toFixed(1)}%
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
