import { useState, useEffect, useCallback, useRef } from 'react';
import { colors, card, font } from '../theme';
import { Archive, FileText, Search, ChevronDown, ChevronRight, Download, Loader, Image, Film, FileArchive, File } from 'lucide-react';

const BASE = '/api';

// ---- Filetype detection ----

const imageExts = new Set(['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.ico']);
const videoExts = new Set(['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm']);
const archiveExts = new Set(['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2', '.xz', '.tgz']);
const docExts = new Set(['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.odt']);

type FileCategory = 'image' | 'video' | 'archive' | 'document' | 'text' | 'unknown';

function getFileCategory(filename: string, isText?: boolean): FileCategory {
  const ext = ('.' + (filename.split('.').pop() || '')).toLowerCase();
  if (imageExts.has(ext)) return 'image';
  if (videoExts.has(ext)) return 'video';
  if (archiveExts.has(ext)) return 'archive';
  if (docExts.has(ext)) return 'document';
  if (isText || textExts.has(ext)) return 'text';
  return 'unknown';
}

function FileCategoryIcon({ category, size = 13 }: { category: FileCategory; size?: number }) {
  const iconColors: Record<FileCategory, string> = {
    image: '#a78bfa',
    video: '#f472b6',
    archive: '#fb923c',
    document: '#60a5fa',
    text: colors.accent,
    unknown: colors.textDim,
  };
  const c = iconColors[category];
  switch (category) {
    case 'image': return <Image size={size} color={c} />;
    case 'video': return <Film size={size} color={c} />;
    case 'archive': return <FileArchive size={size} color={c} />;
    case 'document': return <FileText size={size} color={c} />;
    case 'text': return <FileText size={size} color={c} />;
    default: return <File size={size} color={c} />;
  }
}

// ---- Authenticated image component ----

function AuthImage({ src, alt, style }: { src: string; alt: string; style?: React.CSSProperties }) {
  const [blobUrl, setBlobUrl] = useState<string | null>(null);
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(true);
  const urlRef = useRef<string | null>(null);

  useEffect(() => {
    const token = localStorage.getItem('dd_token');
    const headers: Record<string, string> = {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    fetch(src, { headers })
      .then(r => {
        if (!r.ok) throw new Error(`${r.status}`);
        return r.blob();
      })
      .then(blob => {
        const url = URL.createObjectURL(blob);
        urlRef.current = url;
        setBlobUrl(url);
        setLoading(false);
      })
      .catch(() => { setError(true); setLoading(false); });

    return () => { if (urlRef.current) URL.revokeObjectURL(urlRef.current); };
  }, [src]);

  if (loading) return <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>Loading image...</div>;
  if (error || !blobUrl) return <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>Failed to load image</div>;
  return <img src={blobUrl} alt={alt} style={style} />;
}

// ---- Auth download helper ----

function authDownloadUrl(s3Key: string): string {
  const token = localStorage.getItem('dd_token');
  const base = `${BASE}/files/${s3Key}`;
  return token ? `${base}?token=${encodeURIComponent(token)}` : base;
}

function authMentionFileUrl(mentionId: string): string {
  const token = localStorage.getItem('dd_token');
  const base = `${BASE}/mentions/${mentionId}/file`;
  return token ? `${base}?token=${encodeURIComponent(token)}` : base;
}

// ---- Standalone file viewer ----

function StandaloneFileViewer({ archive }: { archive: ArchiveSummary }) {
  const cat = getFileCategory(archive.file_name);
  const isImage = cat === 'image';
  const isText = cat === 'text';
  const fileUrl = `${BASE}/mentions/${archive.mention_id}/file`;
  const downloadUrl = authMentionFileUrl(archive.mention_id);

  const [content, setContent] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!isText) return;
    setLoading(true);
    const token = localStorage.getItem('dd_token');
    const headers: Record<string, string> = {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    fetch(fileUrl, { headers })
      .then(r => r.ok ? r.text() : '[Failed to load]')
      .then(text => setContent(text.slice(0, 50000)))
      .catch(() => setContent('[Failed to load]'))
      .finally(() => setLoading(false));
  }, [archive.mention_id, isText, fileUrl]);

  return (
    <div style={{ ...card, padding: 16 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <FileCategoryIcon category={cat} size={16} />
        <span style={{ fontSize: 14, fontWeight: 600, color: colors.text, fontFamily: font.mono }}>
          {archive.file_name}
        </span>
        <span style={{ fontSize: 11, color: colors.textMuted }}>{formatSize(archive.file_size)}</span>
        <a
          href={downloadUrl}
          download={archive.file_name}
          style={{
            marginLeft: 'auto', display: 'inline-flex', alignItems: 'center', gap: 4,
            padding: '4px 10px', fontSize: 11, fontWeight: 600,
            background: colors.accent, color: '#fff', borderRadius: 4,
            textDecoration: 'none',
          }}
        >
          <Download size={12} /> Download
        </a>
      </div>

      {isImage && (
        <AuthImage
          src={fileUrl}
          alt={archive.file_name}
          style={{
            maxWidth: '100%', maxHeight: 600, borderRadius: 4,
            border: `1px solid ${colors.border}`,
          }}
        />
      )}

      {isText && (
        loading ? (
          <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>Loading...</div>
        ) : content ? (
          <pre style={{
            fontFamily: font.mono, fontSize: 11, lineHeight: 1.5,
            color: colors.textDim, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
            background: colors.bg, padding: 12, borderRadius: 4,
            border: `1px solid ${colors.border}`,
            maxHeight: 600, overflow: 'auto', margin: 0,
          }}>{content}</pre>
        ) : null
      )}

      {!isImage && !isText && (
        <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>
          {cat === 'video' ? 'Video file' : cat === 'document' ? 'Document' : 'File'} — download to view
        </div>
      )}
    </div>
  );
}

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

function highlightText(text: string | null | undefined, needle: string) {
  if (!text) return text ?? '';
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

interface ArchiveSummary {
  mention_id: string;
  file_name: string;
  file_size: number;
  file_mime: string;
  source_name: string;
  collected_at: string;
  file_count: number;
}

function authedFetch(url: string): Promise<Response> {
  const token = localStorage.getItem('dd_token');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  return fetch(url, { headers });
}

export default function Files() {
  const [query, setQuery] = useState('');
  const [activeQuery, setActiveQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [expandedFile, setExpandedFile] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState<Record<string, string>>({});
  const [contentLoading, setContentLoading] = useState<string | null>(null);

  // Archive browsing state (default view)
  const [archives, setArchives] = useState<ArchiveSummary[]>([]);
  const [selectedArchive, setSelectedArchive] = useState<string | null>(null);
  const [archiveFiles, setArchiveFiles] = useState<SearchResult[]>([]);
  const [archiveLoading, setArchiveLoading] = useState(false);
  const [archivesLoading, setArchivesLoading] = useState(true);

  // Load archive list on mount
  useEffect(() => {
    async function loadArchives() {
      setArchivesLoading(true);
      try {
        const res = await authedFetch(`${BASE}/mentions?has_media=true&page_size=200`);
        if (!res.ok) return;
        const data = await res.json();
        const items = (data.items || [])
          .filter((m: Record<string, unknown>) => {
            const meta = m.metadata as Record<string, unknown> | undefined;
            return meta?.s3_key;
          })
          .map((m: Record<string, unknown>) => {
            const meta = m.metadata as Record<string, unknown> | undefined;
            return {
              mention_id: m.id as string,
              file_name: (meta?.file_name as string) || 'unknown',
              file_size: (meta?.file_size as number) || 0,
              file_mime: (meta?.file_mime as string) || '',
              source_name: (m.source_name as string) || '',
              collected_at: (m.collected_at as string) || '',
              file_count: 0,
            };
          });
        setArchives(items);
      } catch { /* ignore */ }
      setArchivesLoading(false);
    }
    loadArchives();
  }, []);

  // Load files for selected archive
  useEffect(() => {
    if (!selectedArchive) return;
    setArchiveLoading(true);
    setArchiveFiles([]);
    authedFetch(`${BASE}/mentions/${selectedArchive}/archive-contents`)
      .then(r => r.ok ? r.json() : { files: [] })
      .then(data => {
        setArchiveFiles((data.files || []).map((f: Record<string, unknown>) => ({
          ...f,
          id: f.s3_key || f.filename,
          mention_id: selectedArchive,
          archive_name: '',
          source: 'archive',
        })));
      })
      .catch(() => setArchiveFiles([]))
      .finally(() => setArchiveLoading(false));
  }, [selectedArchive]);

  const doSearch = useCallback(async (q: string) => {
    if (!q.trim()) {
      setActiveQuery('');
      setResults([]);
      setTotal(0);
      return;
    }
    setLoading(true);
    setExpandedFile(null);
    try {
      const res = await authedFetch(`${BASE}/extracted-files/search?q=${encodeURIComponent(q)}&limit=100`);
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
        /* Default: archive browser */
        <div style={{ display: 'flex', gap: 16, minHeight: 'calc(100vh - 200px)' }}>
          {/* Archive list */}
          <div style={{ width: 320, flexShrink: 0 }}>
            <div style={{ ...card, padding: 0, maxHeight: 'calc(100vh - 220px)', overflow: 'auto' }}>
              <div style={{ padding: '10px 14px', borderBottom: `1px solid ${colors.border}`, fontSize: 12, fontWeight: 600, color: colors.textDim, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                <Archive size={12} style={{ marginRight: 6, verticalAlign: -1 }} />
                Archives ({archives.length})
              </div>
              {archivesLoading ? (
                <div style={{ padding: 20, textAlign: 'center', color: colors.textMuted }}>Loading...</div>
              ) : archives.map(a => {
                const cat = getFileCategory(a.file_name);
                return (
                  <div
                    key={a.mention_id}
                    onClick={() => setSelectedArchive(selectedArchive === a.mention_id ? null : a.mention_id)}
                    style={{
                      padding: '8px 14px', cursor: 'pointer',
                      borderBottom: `1px solid ${colors.border}`,
                      background: selectedArchive === a.mention_id ? colors.bgHover : 'transparent',
                    }}
                    onMouseEnter={e => { if (selectedArchive !== a.mention_id) (e.currentTarget as HTMLElement).style.background = colors.bgSurface; }}
                    onMouseLeave={e => { if (selectedArchive !== a.mention_id) (e.currentTarget as HTMLElement).style.background = 'transparent'; }}
                  >
                    <div style={{ fontSize: 12, fontFamily: font.mono, color: colors.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', display: 'flex', alignItems: 'center', gap: 6 }}>
                      <FileCategoryIcon category={cat} size={12} />
                      {a.file_name}
                    </div>
                    <div style={{ fontSize: 11, color: colors.textMuted, display: 'flex', gap: 8 }}>
                      <span>{formatSize(a.file_size)}</span>
                      <span>{a.source_name}</span>
                      <span>{new Date(a.collected_at).toLocaleDateString()}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* File list for selected archive */}
          <div style={{ flex: 1, minWidth: 0 }}>
            {!selectedArchive ? (
              <div style={{ ...card, padding: 40, textAlign: 'center', color: colors.textMuted }}>
                <Archive size={28} style={{ marginBottom: 8, opacity: 0.3 }} />
                <div style={{ fontSize: 14 }}>Select an archive to browse its contents</div>
                <div style={{ fontSize: 12, marginTop: 4 }}>Or use the search bar to find files across all archives.</div>
              </div>
            ) : archiveLoading ? (
              <div style={{ ...card, padding: 40, textAlign: 'center', color: colors.textMuted }}>
                <Loader size={20} style={{ animation: 'spin 1s linear infinite' }} />
              </div>
            ) : archiveFiles.length === 0 && selectedArchive ? (
              /* Standalone file — not an archive, show file directly */
              (() => {
                const arch = archives.find(a => a.mention_id === selectedArchive);
                if (!arch) return null;
                return <StandaloneFileViewer archive={arch} />;
              })()
            ) : (
              <div>
                <div style={{ fontSize: 13, color: colors.textDim, marginBottom: 8 }}>
                  {archiveFiles.length} file{archiveFiles.length !== 1 ? 's' : ''}
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  {archiveFiles.map(file => {
                    const fid = file.s3_key || file.filename;
                    const isExpanded = expandedFile === fid;
                    const category = getFileCategory(file.filename, file.is_text);
                    const isText = category === 'text';
                    const isImage = category === 'image';
                    const content = fileContent[fid] || '';
                    const isLoadingContent = contentLoading === fid;

                    return (
                      <div key={fid}>
                        <div
                          style={{
                            display: 'flex', alignItems: 'center', gap: 8, padding: '6px 10px',
                            background: isExpanded ? colors.bgHover : 'transparent',
                            borderRadius: 4, cursor: 'pointer',
                          }}
                          onClick={() => {
                            const id = fid;
                            if (expandedFile === id) { setExpandedFile(null); return; }
                            setExpandedFile(id);
                            if (!fileContent[id] && isText && file.s3_key) {
                              setContentLoading(id);
                              authedFetch(`${BASE}/files/${file.s3_key}`)
                                .then(r => r.ok ? r.text() : '[Failed to load]')
                                .then(text => setFileContent(prev => ({ ...prev, [id]: text })))
                                .catch(() => setFileContent(prev => ({ ...prev, [id]: '[Failed to load]' })))
                                .finally(() => setContentLoading(null));
                            }
                          }}
                        >
                          {isExpanded ? <ChevronDown size={12} color={colors.textMuted} /> : <ChevronRight size={12} color={colors.textMuted} />}
                          <FileCategoryIcon category={category} />
                          <span style={{ fontSize: 12, fontFamily: font.mono, color: colors.text, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {file.filename}
                          </span>
                          <span style={{ fontSize: 11, color: colors.textMuted }}>{formatSize(file.size)}</span>
                          {file.s3_key && (
                            <a href={authDownloadUrl(file.s3_key)} onClick={e => e.stopPropagation()} style={{ color: colors.textMuted, display: 'flex' }} title="Download" download={file.filename}>
                              <Download size={12} />
                            </a>
                          )}
                        </div>
                        {isExpanded && (
                          <div style={{ marginLeft: 32, marginTop: 4, marginBottom: 8 }}>
                            {isImage && file.s3_key ? (
                              <AuthImage
                                src={`${BASE}/files/${file.s3_key}`}
                                alt={file.filename}
                                style={{
                                  maxWidth: '100%', maxHeight: 500, borderRadius: 4,
                                  border: `1px solid ${colors.border}`,
                                }}
                              />
                            ) : isLoadingContent ? (
                              <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>Loading...</div>
                            ) : isText && content ? (
                              <pre style={{
                                fontFamily: font.mono, fontSize: 11, lineHeight: 1.5,
                                color: colors.textDim, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
                                background: colors.bg, padding: 12, borderRadius: 4,
                                border: `1px solid ${colors.border}`,
                                maxHeight: 500, overflow: 'auto', margin: 0,
                              }}>{content}</pre>
                            ) : isText ? (
                              <div style={{ padding: 12, color: colors.textMuted, fontSize: 12, fontStyle: 'italic' }}>No content available</div>
                            ) : (
                              <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>
                                {category === 'video' ? 'Video file' : category === 'archive' ? 'Archive file' : category === 'document' ? 'Document' : 'Binary file'} — download to view
                                {file.s3_key && (
                                  <a href={authDownloadUrl(file.s3_key)} download={file.filename}
                                    style={{ marginLeft: 8, color: colors.accent, fontSize: 12, textDecoration: 'none' }}>
                                    <Download size={11} style={{ verticalAlign: -2, marginRight: 4 }} />Download
                                  </a>
                                )}
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
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
                  const category = getFileCategory(file.filename, file.is_text);
                  const isText = category === 'text';
                  const isImage = category === 'image';
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
                        <FileCategoryIcon category={category} />
                        <span style={{ fontSize: 12, fontFamily: font.mono, color: colors.text, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {highlightText(file.filename, activeQuery)}
                        </span>
                        <span style={{ fontSize: 11, color: colors.textMuted, whiteSpace: 'nowrap' }}>{formatSize(file.size)}</span>
                        {file.s3_key && (
                          <a href={authDownloadUrl(file.s3_key)} onClick={e => e.stopPropagation()} style={{ color: colors.textMuted, display: 'flex' }} title="Download" download={file.filename}>
                            <Download size={12} />
                          </a>
                        )}
                      </div>

                      {/* Preview snippet */}
                      {!isExpanded && file.preview && (
                        <div style={{ marginLeft: 40, marginBottom: 4, fontSize: 11, color: colors.textMuted, fontFamily: font.mono, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '80%' }}>
                          {highlightText((file.preview || '').slice(0, 120), activeQuery)}
                        </div>
                      )}

                      {/* Full content */}
                      {isExpanded && (
                        <div style={{ marginLeft: 32, marginTop: 4, marginBottom: 8 }}>
                          {isImage && file.s3_key ? (
                            <AuthImage
                              src={`${BASE}/files/${file.s3_key}`}
                              alt={file.filename}
                              style={{
                                maxWidth: '100%', maxHeight: 500, borderRadius: 4,
                                border: `1px solid ${colors.border}`,
                              }}
                            />
                          ) : isLoadingContent ? (
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
                              {category === 'video' ? 'Video file' : category === 'archive' ? 'Archive file' : category === 'document' ? 'Document' : 'Binary file'} ({file.extension || 'unknown'}) — {file.s3_key ? '' : 'not available'}
                              {file.s3_key && (
                                <a href={authDownloadUrl(file.s3_key)} download={file.filename}
                                  style={{ color: colors.accent, fontSize: 12, textDecoration: 'none' }}>
                                  <Download size={11} style={{ verticalAlign: -2, marginRight: 4 }} />Download to view
                                </a>
                              )}
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
