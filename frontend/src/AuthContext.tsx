import { createContext, useContext, useState, useCallback, useEffect } from 'react';
import type { ReactNode } from 'react';

interface AuthContextValue {
  token: string | null;
  login: (token: string) => void;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue>({
  token: null,
  login: () => {},
  logout: () => {},
});

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem('dd_token'));

  const login = useCallback((t: string) => {
    localStorage.setItem('dd_token', t);
    setToken(t);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem('dd_token');
    setToken(null);
  }, []);

  useEffect(() => {
    const handler = (e: CustomEvent) => {
      if (e.detail === 'unauthorized') logout();
    };
    window.addEventListener('auth:logout', handler as EventListener);
    return () => window.removeEventListener('auth:logout', handler as EventListener);
  }, [logout]);

  return (
    <AuthContext.Provider value={{ token, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
