import { useState } from 'react';
import type { FormEvent, CSSProperties } from 'react';
import { KeyRound, CheckCircle, AlertCircle } from 'lucide-react';
import { useAuth } from '../AuthContext';
import { changePassword } from '../api';
import { colors, font } from '../theme';

const pageStyle: CSSProperties = {
  padding: '32px 40px',
  maxWidth: 600,
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
};

const labelStyle: CSSProperties = {
  display: 'block',
  fontSize: 13,
  fontWeight: 500,
  color: colors.textDim,
  marginBottom: 6,
};

const inputStyle: CSSProperties = {
  width: '100%',
  padding: '10px 12px',
  background: colors.bgSurface,
  border: `1px solid ${colors.borderLight}`,
  borderRadius: 6,
  color: colors.text,
  fontSize: 14,
  fontFamily: font.sans,
  outline: 'none',
  boxSizing: 'border-box',
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

export default function Account() {
  const { user } = useAuth();
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setMessage(null);

    if (newPassword !== confirmPassword) {
      setMessage({ type: 'error', text: 'New passwords do not match' });
      return;
    }
    if (newPassword.length < 8) {
      setMessage({ type: 'error', text: 'New password must be at least 8 characters' });
      return;
    }

    setLoading(true);
    try {
      await changePassword(currentPassword, newPassword);
      setMessage({ type: 'success', text: 'Password changed successfully' });
      setCurrentPassword('');
      setNewPassword('');
      setConfirmPassword('');
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to change password';
      setMessage({ type: 'error', text: msg.includes('400') ? 'Current password is incorrect' : msg });
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={pageStyle}>
      <h1 style={headingStyle}>Account Settings</h1>
      <p style={subStyle}>Manage your account credentials</p>

      {user && (
        <div style={{ ...cardStyle, marginBottom: 24 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12 }}>
            <div style={{
              width: 40, height: 40, borderRadius: '50%',
              background: colors.accent, display: 'flex', alignItems: 'center',
              justifyContent: 'center', color: '#fff', fontWeight: 700, fontSize: 16,
            }}>
              {user.username.charAt(0).toUpperCase()}
            </div>
            <div>
              <div style={{ fontSize: 16, fontWeight: 600, color: colors.text }}>{user.username}</div>
              <div style={{ fontSize: 12, color: colors.textMuted, textTransform: 'capitalize' }}>{user.role}</div>
            </div>
          </div>
        </div>
      )}

      <div style={cardStyle}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 20 }}>
          <KeyRound size={18} color={colors.accent} />
          <span style={{ fontSize: 16, fontWeight: 600, color: colors.text }}>Change Password</span>
        </div>

        {message && (
          <div style={msgStyle(message.type)}>
            {message.type === 'success' ? <CheckCircle size={16} /> : <AlertCircle size={16} />}
            {message.text}
          </div>
        )}

        <form onSubmit={handleSubmit}>
          <div style={{ marginBottom: 16 }}>
            <label style={labelStyle}>Current Password</label>
            <input
              style={inputStyle}
              type="password"
              value={currentPassword}
              onChange={e => setCurrentPassword(e.target.value)}
              autoComplete="current-password"
              required
            />
          </div>
          <div style={{ marginBottom: 16 }}>
            <label style={labelStyle}>New Password</label>
            <input
              style={inputStyle}
              type="password"
              value={newPassword}
              onChange={e => setNewPassword(e.target.value)}
              autoComplete="new-password"
              required
              minLength={8}
            />
          </div>
          <div style={{ marginBottom: 24 }}>
            <label style={labelStyle}>Confirm New Password</label>
            <input
              style={inputStyle}
              type="password"
              value={confirmPassword}
              onChange={e => setConfirmPassword(e.target.value)}
              autoComplete="new-password"
              required
              minLength={8}
            />
          </div>
          <button style={{ ...btnStyle, opacity: loading ? 0.7 : 1 }} type="submit" disabled={loading}>
            {loading ? 'Changing...' : 'Change Password'}
          </button>
        </form>
      </div>
    </div>
  );
}
