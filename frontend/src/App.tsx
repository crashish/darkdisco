import { Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider, useAuth } from './AuthContext';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import Findings from './pages/Findings';
import FindingDetail from './pages/FindingDetail';
import Institutions from './pages/Institutions';
import Sources from './pages/Sources';
import SourceDetail from './pages/SourceDetail';
import Mentions from './pages/Mentions';
import Files from './pages/Files';
import Reports from './pages/Reports';
import Login from './pages/Login';
import type { ReactNode } from 'react';

function RequireAuth({ children }: { children: ReactNode }) {
  const { token } = useAuth();
  if (!token) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

export default function App() {
  return (
    <AuthProvider>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route element={<RequireAuth><Layout /></RequireAuth>}>
          <Route path="/" element={<Dashboard />} />
          <Route path="/findings" element={<Findings />} />
          <Route path="/findings/:id" element={<FindingDetail />} />
          <Route path="/institutions" element={<Institutions />} />
          <Route path="/mentions" element={<Mentions />} />
          <Route path="/files" element={<Files />} />
          <Route path="/sources" element={<Sources />} />
          <Route path="/sources/:id" element={<SourceDetail />} />
          <Route path="/reports" element={<Reports />} />
        </Route>
      </Routes>
    </AuthProvider>
  );
}
