import { Routes, Route } from 'react-router-dom';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import Findings from './pages/Findings';
import Institutions from './pages/Institutions';
import Sources from './pages/Sources';

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/findings" element={<Findings />} />
        <Route path="/institutions" element={<Institutions />} />
        <Route path="/sources" element={<Sources />} />
      </Route>
    </Routes>
  );
}
