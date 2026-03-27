import { createContext, useContext, useState, useCallback, useEffect } from 'react';
import type { ReactNode } from 'react';

export interface UserInfo {
  id: string;
  username: string;
  role: string;
}

interface AuthContextValue {
  token: string | null;
  user: UserInfo | null;
  login: (token: string) => void;
  logout: () => void;
  refreshUser: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue>({
  token: null,
  user: null,
  login: () => {},
  logout: () => {},
  refreshUser: async () => {},
});

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem('dd_token'));
  const [user, setUser] = useState<UserInfo | null>(null);

  const logout = useCallback(() => {
    localStorage.removeItem('dd_token');
    setToken(null);
    setUser(null);
  }, []);

  const login = useCallback((t: string) => {
    localStorage.setItem('dd_token', t);
    setToken(t);
  }, []);

  const refreshUser = useCallback(async () => {
    const stored = localStorage.getItem('dd_token');
    if (!stored) {
      setUser(null);
      return;
    }
    try {
      const res = await fetch('/api/auth/me', {
        headers: { 'Authorization': `Bearer ${stored}` },
      });
      if (res.status === 401) {
        localStorage.removeItem('dd_token');
        setToken(null);
        setUser(null);
        return;
      }
      if (res.ok) {
        const data = await res.json();
        setUser({ id: data.id, username: data.username, role: data.role });
      }
    } catch {
      // Network error — keep token but don't set user
    }
  }, []);

  // Validate token and fetch user info on mount and when token changes
  useEffect(() => {
    if (token) {
      refreshUser();
    } else {
      setUser(null);
    }
  }, [token, refreshUser]);

  useEffect(() => {
    const handler = (e: CustomEvent) => {
      if (e.detail === 'unauthorized') logout();
    };
    window.addEventListener('auth:logout', handler as EventListener);
    return () => window.removeEventListener('auth:logout', handler as EventListener);
  }, [logout]);

  return (
    <AuthContext.Provider value={{ token, user, login, logout, refreshUser }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
