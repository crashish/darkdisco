import { useState } from 'react';
import type { FormEvent, CSSProperties } from 'react';
import { useNavigate } from 'react-router-dom';
import { Disc3 } from 'lucide-react';
import { useAuth } from '../AuthContext';
import { colors, font } from '../theme';

const pageStyle: CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  minHeight: '100vh',
  background: colors.bg,
  fontFamily: font.sans,
};

const cardStyle: CSSProperties = {
  width: 380,
  background: colors.bgCard,
  border: `1px solid ${colors.border}`,
  borderRadius: 12,
  padding: '40px 32px',
};

const brandStyle: CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  gap: 10,
  fontSize: 24,
  fontWeight: 700,
  color: colors.text,
  marginBottom: 32,
  letterSpacing: '-0.02em',
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
  width: '100%',
  padding: '10px 0',
  background: colors.accent,
  color: '#fff',
  border: 'none',
  borderRadius: 6,
  fontSize: 14,
  fontWeight: 600,
  cursor: 'pointer',
  fontFamily: font.sans,
  marginTop: 8,
};

const errorStyle: CSSProperties = {
  background: colors.criticalBg,
  color: colors.critical,
  padding: '8px 12px',
  borderRadius: 6,
  fontSize: 13,
  marginBottom: 16,
};

export default function Login() {
  const { login } = useAuth();
  const navigate = useNavigate();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null);
        throw new Error(data?.detail || 'Invalid credentials');
      }
      const data = await res.json();
      login(data.access_token);
      navigate('/', { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Login failed');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={pageStyle}>
      <div style={cardStyle}>
        <div style={brandStyle}>
          <Disc3 size={28} color={colors.accent} />
          DarkDisco
        </div>
        {error && <div style={errorStyle}>{error}</div>}
        <form onSubmit={handleSubmit}>
          <div style={{ marginBottom: 16 }}>
            <label style={labelStyle}>Username</label>
            <input
              style={inputStyle}
              type="text"
              value={username}
              onChange={e => setUsername(e.target.value)}
              autoComplete="username"
              required
              autoFocus
            />
          </div>
          <div style={{ marginBottom: 20 }}>
            <label style={labelStyle}>Password</label>
            <input
              style={inputStyle}
              type="password"
              value={password}
              onChange={e => setPassword(e.target.value)}
              autoComplete="current-password"
              required
            />
          </div>
          <button style={{ ...btnStyle, opacity: loading ? 0.7 : 1 }} type="submit" disabled={loading}>
            {loading ? 'Signing in...' : 'Sign in'}
          </button>
        </form>
      </div>
    </div>
  );
}
