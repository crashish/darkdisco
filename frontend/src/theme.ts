import type { CSSProperties } from 'react';

export const colors = {
  bg: '#0a0e17',
  bgCard: '#111827',
  bgSurface: '#1a2233',
  bgHover: '#1e293b',
  border: '#1e293b',
  borderLight: '#2d3a4d',
  text: '#e2e8f0',
  textDim: '#94a3b8',
  textMuted: '#64748b',
  accent: '#6366f1',
  accentHover: '#818cf8',

  critical: '#ef4444',
  criticalBg: 'rgba(239, 68, 68, 0.12)',
  high: '#f97316',
  highBg: 'rgba(249, 115, 22, 0.12)',
  medium: '#eab308',
  mediumBg: 'rgba(234, 179, 8, 0.12)',
  low: '#3b82f6',
  lowBg: 'rgba(59, 130, 246, 0.12)',
  info: '#64748b',
  infoBg: 'rgba(100, 116, 139, 0.12)',

  healthy: '#22c55e',
  healthyBg: 'rgba(34, 197, 94, 0.12)',
  degraded: '#eab308',
  degradedBg: 'rgba(234, 179, 8, 0.12)',
  offline: '#ef4444',
  offlineBg: 'rgba(239, 68, 68, 0.12)',

  statusFalsePositive: '#64748b',
  statusDismissed: '#64748b',
  statusConfirmed: '#22c55e',
};

export const severityColor = (s: string) => colors[s as keyof typeof colors] || colors.textDim;
export const severityBg = (s: string) => colors[`${s}Bg` as keyof typeof colors] || 'transparent';
export const healthColor = (s: string) => colors[s as keyof typeof colors] || colors.textDim;
export const healthBg = (s: string) => colors[`${s}Bg` as keyof typeof colors] || 'transparent';
const statusColorMap: Record<string, string> = {
  new: '#6366f1',
  reviewing: '#eab308',
  escalated: '#ef4444',
  false_positive: '#64748b',
  resolved: '#22c55e',
};
export const statusColor = (s: string) => statusColorMap[s] || colors.textDim;

const statusLabelMap: Record<string, string> = {
  new: 'New',
  reviewing: 'Reviewing',
  escalated: 'Escalated',
  false_positive: 'False Positive',
  resolved: 'Resolved',
};
export const statusLabel = (s: string) => statusLabelMap[s] || s;

export const font = {
  sans: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
  mono: "'JetBrains Mono', 'Fira Code', monospace",
};

export const card: CSSProperties = {
  background: colors.bgCard,
  border: `1px solid ${colors.border}`,
  borderRadius: 8,
  padding: '20px',
};

export const badge = (color: string, bg: string): CSSProperties => ({
  display: 'inline-flex',
  alignItems: 'center',
  gap: 4,
  padding: '2px 10px',
  borderRadius: 9999,
  fontSize: 12,
  fontWeight: 600,
  color,
  background: bg,
  textTransform: 'uppercase',
  letterSpacing: '0.025em',
});
