import { useState, useEffect } from 'react';
import type { FormEvent, CSSProperties } from 'react';
import { Settings, CheckCircle, AlertCircle, HardDrive } from 'lucide-react';
import { fetchSystemSettings, updateSystemSetting } from '../api';
import type { SystemSetting } from '../api';
import { colors, font } from '../theme';

const pageStyle: CSSProperties = {
  padding: '32px 40px',
  maxWidth: 700,
};

const headingStyle: CSSProperties = {
  fontSize: 22,
  fontWeight: 700,
  color: colors.text,
  marginBottom: 8,
};

const subStyle: CSSProperties = {
  fontSize: 14,
  color: colors.textDim,
  marginBottom: 32,
};

const cardStyle: CSSProperties = {
  background: colors.bgCard,
  border: `1px solid ${colors.border}`,
  borderRadius: 10,
  padding: '28px 24px',
  marginBottom: 24,
};

const labelStyle: CSSProperties = {
  display: 'block',
  fontSize: 13,
  fontWeight: 500,
  color: colors.textDim,
  marginBottom: 6,
};

const inputStyle: CSSProperties = {
  width: 120,
  padding: '10px 12px',
  background: colors.bgSurface,
  border: `1px solid ${colors.borderLight}`,
  borderRadius: 6,
  color: colors.text,
  fontSize: 14,
  fontFamily: font.sans,
  outline: 'none',
  boxSizing: 'border-box',
  textAlign: 'right' as const,
};

const btnStyle: CSSProperties = {
  padding: '10px 24px',
  background: colors.accent,
  color: '#fff',
  border: 'none',
  borderRadius: 6,
  fontSize: 14,
  fontWeight: 600,
  cursor: 'pointer',
  fontFamily: font.sans,
  display: 'inline-flex',
  alignItems: 'center',
  gap: 8,
};

const msgStyle = (type: 'success' | 'error'): CSSProperties => ({
  padding: '10px 14px',
  borderRadius: 6,
  fontSize: 13,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  marginBottom: 20,
  background: type === 'success' ? colors.healthyBg : colors.criticalBg,
  color: type === 'success' ? colors.healthy : colors.critical,
});

function bytesToGB(bytes: number): string {
  return (bytes / (1024 * 1024 * 1024)).toFixed(1);
}

function gbToBytes(gb: number): string {
  return String(Math.round(gb * 1024 * 1024 * 1024));
}

export default function SystemSettings() {
  const [settings, setSettings] = useState<SystemSetting[]>([]);
  const [downloadLimitGB, setDownloadLimitGB] = useState('5.0');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);

  useEffect(() => {
    fetchSystemSettings()
      .then(data => {
        setSettings(data);
        const dlSetting = data.find(s => s.key === 'max_download_size_bytes');
        if (dlSetting) {
          setDownloadLimitGB(bytesToGB(Number(dlSetting.value)));
        }
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  async function handleSave(e: FormEvent) {
    e.preventDefault();
    setMessage(null);
    const gb = parseFloat(downloadLimitGB);
    if (isNaN(gb) || gb < 0.1 || gb > 50) {
      setMessage({ type: 'error', text: 'Enter a value between 0.1 and 50 GB' });
      return;
    }
    setSaving(true);
    try {
      const updated = await updateSystemSetting('max_download_size_bytes', gbToBytes(gb));
      setSettings(prev => prev.map(s => s.key === updated.key ? updated : s));
      setDownloadLimitGB(bytesToGB(Number(updated.value)));
      setMessage({ type: 'success', text: 'Download size limit updated' });
    } catch {
      setMessage({ type: 'error', text: 'Failed to update setting' });
    } finally {
      setSaving(false);
    }
  }

  if (loading) {
    return (
      <div style={pageStyle}>
        <h1 style={headingStyle}>System Settings</h1>
        <p style={subStyle}>Loading...</p>
      </div>
    );
  }

  return (
    <div style={pageStyle}>
      <h1 style={headingStyle}>System Settings</h1>
      <p style={subStyle}>Configure system-wide operational parameters</p>

      <div style={cardStyle}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 20 }}>
          <HardDrive size={18} color={colors.accent} />
          <span style={{ fontSize: 16, fontWeight: 600, color: colors.text }}>Download Size Limit</span>
        </div>

        <p style={{ fontSize: 13, color: colors.textDim, marginBottom: 16, lineHeight: 1.5 }}>
          Maximum file size for downloads from Telegram channels and stealer log archives.
          Files larger than this limit will be skipped during ingestion.
          Stealer log archives (e.g. Trident Cloud) can be several GB.
        </p>

        {message && (
          <div style={msgStyle(message.type)}>
            {message.type === 'success' ? <CheckCircle size={16} /> : <AlertCircle size={16} />}
            {message.text}
          </div>
        )}

        <form onSubmit={handleSave}>
          <div style={{ marginBottom: 20 }}>
            <label style={labelStyle}>Maximum download size</label>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <input
                style={inputStyle}
                type="number"
                step="0.1"
                min="0.1"
                max="50"
                value={downloadLimitGB}
                onChange={e => setDownloadLimitGB(e.target.value)}
              />
              <span style={{ fontSize: 14, color: colors.textDim }}>GB</span>
            </div>
          </div>
          <button style={{ ...btnStyle, opacity: saving ? 0.7 : 1 }} type="submit" disabled={saving}>
            {saving ? 'Saving...' : 'Save'}
          </button>
        </form>
      </div>

      {settings.length > 0 && (
        <div style={{ ...cardStyle, padding: '20px 24px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16 }}>
            <Settings size={18} color={colors.textDim} />
            <span style={{ fontSize: 14, fontWeight: 600, color: colors.text }}>All Settings</span>
          </div>
          <table style={{ width: '100%', fontSize: 13, borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                <th style={{ textAlign: 'left', padding: '8px 0', color: colors.textDim, fontWeight: 500 }}>Key</th>
                <th style={{ textAlign: 'right', padding: '8px 0', color: colors.textDim, fontWeight: 500 }}>Value</th>
              </tr>
            </thead>
            <tbody>
              {settings.map(s => (
                <tr key={s.key} style={{ borderBottom: `1px solid ${colors.border}` }}>
                  <td style={{ padding: '8px 0', color: colors.text }}>{s.key}</td>
                  <td style={{ padding: '8px 0', color: colors.textDim, textAlign: 'right', fontFamily: 'monospace' }}>{s.value}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
