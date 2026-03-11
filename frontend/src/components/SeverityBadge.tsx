import { severityColor, severityBg, badge } from '../theme';
import { AlertTriangle, AlertCircle, Info, Shield, ShieldAlert } from 'lucide-react';
import type { Severity } from '../types';

const icons: Record<Severity, typeof AlertTriangle> = {
  critical: ShieldAlert,
  high: AlertTriangle,
  medium: AlertCircle,
  low: Shield,
  info: Info,
};

export default function SeverityBadge({ severity }: { severity: Severity }) {
  const Icon = icons[severity];
  const color = severityColor(severity);
  const bg = severityBg(severity);
  return (
    <span style={badge(color, bg)}>
      <Icon size={12} />
      {severity}
    </span>
  );
}
