import { Routes, Route } from 'react-router-dom';

export default function App() {
  return (
    <div className="app">
      <nav className="navbar">
        <span className="nav-brand">DarkDisco</span>
        <div className="nav-links">
          <a href="/">Dashboard</a>
          <a href="/findings">Findings</a>
          <a href="/institutions">Institutions</a>
          <a href="/sources">Sources</a>
        </div>
      </nav>
      <main className="content">
        <Routes>
          <Route path="/" element={<div>Dashboard — coming soon</div>} />
          <Route path="/findings" element={<div>Findings — coming soon</div>} />
          <Route path="/institutions" element={<div>Institutions — coming soon</div>} />
          <Route path="/sources" element={<div>Sources — coming soon</div>} />
        </Routes>
      </main>
    </div>
  );
}
