import { useEffect, useState } from 'react';
import { Download, Loader2, CheckCircle2, XCircle, Clock, HardDrive, FileArchive, AlertTriangle } from 'lucide-react';
import { fetchDownloadStatus } from '../api';
import { colors, card, font } from '../theme';
import type { DownloadQueueStatus, DownloadTaskInfo } from '../types';
import type { CSSProperties } from 'react';

const POLL_INTERVAL = 5000;

const statCard = (): CSSProperties => ({
  ...card,
  display: 'flex',
  alignItems: 'center',
  gap: 16,
  flex: '1 1 0',
  minWidth: 180,
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

const thStyle: CSSProperties = {
  textAlign: 'left',
  padding: '8px 12px',
  color: colors.textMuted,
  fontWeight: 500,
  fontSize: 11,
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
};

const tdStyle: CSSProperties = {
  padding: '10px 12px',
  fontSize: 13,
};

function StatusIndicator({ status }: { status: string }) {
  switch (status) {
    case 'active':
      return (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: colors.accent, fontSize: 12, fontWeight: 600 }}>
          <Loader2 size={14} style={{ animation: 'spin 1s linear infinite' }} />
          Extracting
        </span>
      );
    case 'pending':
      return (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: colors.medium, fontSize: 12, fontWeight: 600 }}>
          <Clock size={14} />
          Queued
        </span>
      );
    case 'success':
      return (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: colors.healthy, fontSize: 12, fontWeight: 600 }}>
          <CheckCircle2 size={14} />
          Complete
        </span>
      );
    case 'error':
      return (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: colors.critical, fontSize: 12, fontWeight: 600 }}>
          <XCircle size={14} />
          Failed
        </span>
      );
    default:
      return (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: colors.textDim, fontSize: 12, fontWeight: 600 }}>
          <Clock size={14} />
          {status}
        </span>
      );
  }
}

function TaskRow({ task }: { task: DownloadTaskInfo }) {
  return (
    <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
      <td style={tdStyle}><StatusIndicator status={task.status} /></td>
      <td style={{ ...tdStyle, fontFamily: font.mono, fontSize: 12 }}>
        {task.filename || 'unknown'}
      </td>
      <td style={{ ...tdStyle, fontFamily: font.mono, fontSize: 11, color: colors.textMuted }}>
        {task.mention_id ? task.mention_id.slice(0, 12) + '...' : '-'}
      </td>
      <td style={{ ...tdStyle, color: colors.textDim }}>
        {task.files_extracted != null ? task.files_extracted : '-'}
      </td>
      <td style={{ ...tdStyle, color: colors.textMuted, fontSize: 12 }}>
        {task.completed_at ? timeAgo(task.completed_at) : task.started_at ? timeAgo(task.started_at) : '-'}
      </td>
    </tr>
  );
}

export default function Downloads() {
  const [data, setData] = useState<DownloadQueueStatus | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    let active = true;

    const poll = () => {
      fetchDownloadStatus()
        .then(d => { if (active) { setData(d); setError(false); } })
        .catch(() => { if (active) setError(true); });
    };

    poll();
    const timer = setInterval(poll, POLL_INTERVAL);
    return () => { active = false; clearInterval(timer); };
  }, []);

  if (!data && !error) {
    return <div style={{ color: colors.textDim, padding: 40 }}>Loading...</div>;
  }

  const stats = data?.stats ?? { total_pending: 0, total_stored: 0, total_errors: 0, total_extracted: 0 };
  const current = data?.current ?? null;
  const pending = data?.pending ?? [];
  const recent = data?.recent ?? [];

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 700, marginBottom: 4 }}>Downloads</h1>
      <p style={{ color: colors.textDim, fontSize: 14, marginBottom: 28 }}>
        File download and extraction queue status
      </p>

      {error && (
        <div style={{ ...card, marginBottom: 16, borderColor: colors.critical, color: colors.critical, fontSize: 13 }}>
          Failed to fetch download status. Retrying...
        </div>
      )}

      {/* Stat cards */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 28 }}>
        <div style={statCard()}>
          <div style={iconBox(colors.mediumBg)}><Clock size={22} color={colors.medium} /></div>
          <div>
            <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1 }}>{stats.total_pending}</div>
            <div style={{ fontSize: 12, color: colors.textDim, marginTop: 2 }}>Pending</div>
          </div>
        </div>
        <div style={statCard()}>
          <div style={iconBox('rgba(99,102,241,0.12)')}><HardDrive size={22} color={colors.accent} /></div>
          <div>
            <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1 }}>{stats.total_stored}</div>
            <div style={{ fontSize: 12, color: colors.textDim, marginTop: 2 }}>Files Stored</div>
          </div>
        </div>
        <div style={statCard()}>
          <div style={iconBox(colors.healthyBg)}><FileArchive size={22} color={colors.healthy} /></div>
          <div>
            <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1 }}>{stats.total_extracted}</div>
            <div style={{ fontSize: 12, color: colors.textDim, marginTop: 2 }}>Extracted</div>
          </div>
        </div>
        <div style={statCard()}>
          <div style={iconBox(colors.criticalBg)}><AlertTriangle size={22} color={colors.critical} /></div>
          <div>
            <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1 }}>{stats.total_errors}</div>
            <div style={{ fontSize: 12, color: colors.textDim, marginTop: 2 }}>Errors</div>
          </div>
        </div>
      </div>

      {/* Current download */}
      <div style={{ ...card, marginBottom: 20 }}>
        <h3 style={{ fontSize: 14, fontWeight: 600, color: colors.textDim, marginBottom: 16, display: 'flex', alignItems: 'center', gap: 8 }}>
          <Download size={16} />
          Current Download
        </h3>
        {current ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: 16, padding: '12px 0' }}>
            <Loader2 size={20} color={colors.accent} style={{ animation: 'spin 1s linear infinite' }} />
            <div>
              <div style={{ fontFamily: font.mono, fontSize: 13 }}>{current.filename || 'unknown'}</div>
              <div style={{ fontSize: 12, color: colors.textMuted, marginTop: 2 }}>
                Mention: {current.mention_id || '-'}
              </div>
            </div>
          </div>
        ) : (
          <div style={{ color: colors.textMuted, fontSize: 13, padding: '12px 0' }}>
            No active downloads
          </div>
        )}
      </div>

      {/* Pending queue */}
      {pending.length > 0 && (
        <div style={{ ...card, marginBottom: 20 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: colors.textDim, marginBottom: 16, display: 'flex', alignItems: 'center', gap: 8 }}>
            <Clock size={16} />
            Pending Queue ({pending.length})
          </h3>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                <th style={thStyle}>Status</th>
                <th style={thStyle}>Filename</th>
                <th style={thStyle}>Mention</th>
                <th style={thStyle}>Files</th>
                <th style={thStyle}>Time</th>
              </tr>
            </thead>
            <tbody>
              {pending.map((t, i) => <TaskRow key={t.task_id || i} task={t} />)}
            </tbody>
          </table>
        </div>
      )}

      {/* Recent completions */}
      <div style={card}>
        <h3 style={{ fontSize: 14, fontWeight: 600, color: colors.textDim, marginBottom: 16, display: 'flex', alignItems: 'center', gap: 8 }}>
          <CheckCircle2 size={16} />
          Recent Downloads ({recent.length})
        </h3>
        {recent.length > 0 ? (
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                <th style={thStyle}>Status</th>
                <th style={thStyle}>Filename</th>
                <th style={thStyle}>Mention</th>
                <th style={thStyle}>Files</th>
                <th style={thStyle}>Completed</th>
              </tr>
            </thead>
            <tbody>
              {recent.map((t, i) => <TaskRow key={t.mention_id || i} task={t} />)}
            </tbody>
          </table>
        ) : (
          <div style={{ color: colors.textMuted, fontSize: 13, padding: '12px 0' }}>
            No recent downloads
          </div>
        )}
      </div>

      {/* CSS for spin animation */}
      <style>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
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
