import { useState, useEffect, useCallback } from 'react';
import { colors, card, font } from '../theme';
import { Archive, FileText, Search, ChevronDown, ChevronRight, Download, Loader } from 'lucide-react';

const BASE = '/api';

interface SearchResult {
  id: string;
  mention_id: string;
  filename: string;
  size: number;
  extension: string;
  is_text: boolean;
  preview: string;
  s3_key: string;
  archive_name: string;
  source: string;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function highlightText(text: string, needle: string) {
  if (!needle) return text;
  const parts: (string | JSX.Element)[] = [];
  const lower = text.toLowerCase();
  const n = needle.toLowerCase();
  let last = 0, idx = lower.indexOf(n);
  let key = 0;
  while (idx !== -1) {
    if (idx > last) parts.push(text.slice(last, idx));
    parts.push(<span key={key++} style={{ background: 'rgba(234, 179, 8, 0.4)', borderRadius: 2, padding: '0 1px' }}>{text.slice(idx, idx + n.length)}</span>);
    last = idx + n.length;
    idx = lower.indexOf(n, last);
  }
  if (last < text.length) parts.push(text.slice(last));
  return <>{parts}</>;
}

const textExts = new Set(['.txt', '.csv', '.log', '.json', '.xml', '.html', '.sql', '.cfg', '.conf', '.ini', '.env', '.yml', '.yaml', '.py', '.js', '.php']);

export default function Files() {
  const [query, setQuery] = useState('');
  const [activeQuery, setActiveQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [expandedFile, setExpandedFile] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState<Record<string, string>>({});
  const [contentLoading, setContentLoading] = useState<string | null>(null);

  const doSearch = useCallback(async (q: string) => {
    if (!q.trim()) {
      setResults([]);
      setTotal(0);
      return;
    }
    setLoading(true);
    setExpandedFile(null);
    try {
      const token = localStorage.getItem('dd_token');
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;
      const res = await fetch(`${BASE}/extracted-files/search?q=${encodeURIComponent(q)}&limit=100`, { headers });
      if (!res.ok) throw new Error(`${res.status}`);
      const data = await res.json();
      setResults(data.files || []);
      setTotal(data.total || 0);
    } catch { setResults([]); setTotal(0); }
    setLoading(false);
  }, []);

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setActiveQuery(query);
    doSearch(query);
  }

  // Fetch file content on demand from S3
  async function loadFileContent(file: SearchResult) {
    if (file.preview && file.source === 'extracted_files') {
      // Use preview from API (already has content snippet)
      setFileContent(prev => ({ ...prev, [file.id]: file.preview }));
    }
    if (!file.s3_key) return;
    setContentLoading(file.id);
    try {
      const token = localStorage.getItem('dd_token');
      const headers: Record<string, string> = {};
      if (token) headers['Authorization'] = `Bearer ${token}`;
      const res = await fetch(`${BASE}/files/${file.s3_key}`, { headers });
      if (!res.ok) throw new Error(`${res.status}`);
      const text = await res.text();
      setFileContent(prev => ({ ...prev, [file.id]: text }));
    } catch {
      setFileContent(prev => ({ ...prev, [file.id]: file.preview || '[Failed to load]' }));
    }
    setContentLoading(null);
  }

  function handleExpandFile(file: SearchResult) {
    if (expandedFile === file.id) {
      setExpandedFile(null);
      return;
    }
    setExpandedFile(file.id);
    const isText = file.is_text || textExts.has('.' + (file.extension || ''));
    if (!fileContent[file.id] && isText) {
      loadFileContent(file);
    }
  }

  // Group results by archive
  const grouped = results.reduce<Record<string, { archive: string; files: SearchResult[] }>>((acc, file) => {
    const key = file.mention_id;
    if (!acc[key]) acc[key] = { archive: file.archive_name || 'Unknown archive', files: [] };
    acc[key].files.push(file);
    return acc;
  }, {});

  return (
    <div>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, marginBottom: 4 }}>File Browser</h1>
        <p style={{ fontSize: 13, color: colors.textMuted, margin: 0 }}>
          Search across all extracted archive contents — filenames and file content.
        </p>
      </div>

      {/* Global search */}
      <form onSubmit={handleSubmit} style={{ display: 'flex', gap: 8, marginBottom: 20 }}>
        <div style={{ position: 'relative', flex: 1 }}>
          <Search size={16} style={{ position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)', color: colors.textMuted }} />
          <input
            type="text"
            placeholder="Search filenames and file contents across all archives..."
            value={query}
            onChange={e => setQuery(e.target.value)}
            style={{
              width: '100%', padding: '10px 14px 10px 36px', fontSize: 14,
              background: colors.bgSurface, border: `1px solid ${colors.border}`,
              borderRadius: 8, color: colors.text, outline: 'none',
            }}
          />
        </div>
        <button type="submit" style={{
          padding: '10px 20px', fontSize: 14, fontWeight: 600,
          background: colors.accent, color: 'white', border: 'none',
          borderRadius: 8, cursor: 'pointer',
        }}>
          Search
        </button>
        {activeQuery && (
          <button type="button" onClick={() => { setQuery(''); setActiveQuery(''); setResults([]); setTotal(0); }} style={{
            padding: '10px 14px', fontSize: 13, background: 'none',
            border: `1px solid ${colors.border}`, borderRadius: 8,
            color: colors.textDim, cursor: 'pointer',
          }}>
            Clear
          </button>
        )}
      </form>

      {/* Results */}
      {loading ? (
        <div style={{ ...card, padding: 40, textAlign: 'center', color: colors.textMuted }}>
          <Loader size={20} style={{ animation: 'spin 1s linear infinite', marginBottom: 8 }} />
          <div>Searching...</div>
        </div>
      ) : !activeQuery ? (
        <div style={{ ...card, padding: 60, textAlign: 'center', color: colors.textMuted }}>
          <Search size={32} style={{ marginBottom: 12, opacity: 0.3 }} />
          <div style={{ fontSize: 15, marginBottom: 4 }}>Search extracted archive contents</div>
          <div style={{ fontSize: 12 }}>
            Enter a search term to find files by name or content across all collected archives.
            <br />Try searching for domains, email addresses, passwords, or credential patterns.
          </div>
        </div>
      ) : results.length === 0 ? (
        <div style={{ ...card, padding: 40, textAlign: 'center', color: colors.textMuted }}>
          <div style={{ fontSize: 14 }}>No results for &ldquo;{activeQuery}&rdquo;</div>
          <div style={{ fontSize: 12, marginTop: 4 }}>Try a different search term or check if archives have been extracted.</div>
        </div>
      ) : (
        <div>
          <div style={{ fontSize: 13, color: colors.textDim, marginBottom: 12 }}>
            {total} result{total !== 1 ? 's' : ''} across {Object.keys(grouped).length} archive{Object.keys(grouped).length !== 1 ? 's' : ''}
          </div>

          {Object.entries(grouped).map(([mentionId, group]) => (
            <div key={mentionId} style={{ ...card, marginBottom: 12, padding: 0 }}>
              {/* Archive header */}
              <div style={{
                padding: '10px 16px', borderBottom: `1px solid ${colors.border}`,
                display: 'flex', alignItems: 'center', gap: 8,
                fontSize: 12, color: colors.textDim,
              }}>
                <Archive size={13} />
                <span style={{ fontFamily: font.mono, color: colors.text, fontWeight: 600 }}>
                  {group.archive}
                </span>
                <span>— {group.files.length} match{group.files.length !== 1 ? 'es' : ''}</span>
              </div>

              {/* Files in this archive */}
              <div style={{ padding: '4px 8px' }}>
                {group.files.map(file => {
                  const isExpanded = expandedFile === file.id;
                  const isText = file.is_text || textExts.has('.' + (file.extension || ''));
                  const content = fileContent[file.id] || '';
                  const isLoadingContent = contentLoading === file.id;

                  return (
                    <div key={file.id}>
                      <div
                        style={{
                          display: 'flex', alignItems: 'center', gap: 8, padding: '7px 8px',
                          background: isExpanded ? colors.bgHover : 'transparent',
                          borderRadius: 4, cursor: 'pointer', transition: 'background 0.1s',
                        }}
                        onClick={() => handleExpandFile(file)}
                        onMouseEnter={e => { if (!isExpanded) (e.currentTarget as HTMLElement).style.background = colors.bgSurface; }}
                        onMouseLeave={e => { if (!isExpanded) (e.currentTarget as HTMLElement).style.background = 'transparent'; }}
                      >
                        {isExpanded ? <ChevronDown size={12} color={colors.textMuted} /> : <ChevronRight size={12} color={colors.textMuted} />}
                        <FileText size={13} color={isText ? colors.accent : colors.textDim} />
                        <span style={{ fontSize: 12, fontFamily: font.mono, color: colors.text, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {highlightText(file.filename, activeQuery)}
                        </span>
                        <span style={{ fontSize: 11, color: colors.textMuted, whiteSpace: 'nowrap' }}>{formatSize(file.size)}</span>
                        {file.s3_key && (
                          <a href={`${BASE}/files/${file.s3_key}`} onClick={e => e.stopPropagation()} style={{ color: colors.textMuted, display: 'flex' }} title="Download">
                            <Download size={12} />
                          </a>
                        )}
                      </div>

                      {/* Preview snippet */}
                      {!isExpanded && file.preview && (
                        <div style={{ marginLeft: 40, marginBottom: 4, fontSize: 11, color: colors.textMuted, fontFamily: font.mono, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '80%' }}>
                          {highlightText(file.preview.slice(0, 120), activeQuery)}
                        </div>
                      )}

                      {/* Full content */}
                      {isExpanded && (
                        <div style={{ marginLeft: 32, marginTop: 4, marginBottom: 8 }}>
                          {isLoadingContent ? (
                            <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>Loading content...</div>
                          ) : isText && content ? (
                            <pre style={{
                              fontFamily: font.mono, fontSize: 11, lineHeight: 1.5,
                              color: colors.textDim, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
                              background: colors.bg, padding: 12, borderRadius: 4,
                              border: `1px solid ${colors.border}`,
                              maxHeight: 500, overflow: 'auto', margin: 0,
                            }}>
                              {highlightText(content, activeQuery)}
                            </pre>
                          ) : isText ? (
                            <div style={{ padding: 12, color: colors.textMuted, fontSize: 12, fontStyle: 'italic' }}>
                              No content available — file may need extraction
                            </div>
                          ) : (
                            <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>
                              Binary file ({file.extension || 'unknown'}) — {file.s3_key ? 'download to view' : 'not available'}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      )}

      <style>{`@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}
