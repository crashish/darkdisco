import { statusColor, colors, badge as makeBadge } from '../theme';
import type { FindingStatus } from '../types';

export default function StatusBadge({ status }: { status: FindingStatus }) {
  const color = statusColor(status);
  return (
    <span style={makeBadge(color, `${color}1a`)}>
      <span style={{ width: 6, height: 6, borderRadius: '50%', background: color, display: 'inline-block' }} />
      {status}
    </span>
  );
}
