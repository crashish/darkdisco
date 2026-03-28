import { useState, useEffect, useCallback, useRef } from 'react';
import { colors, card, font } from '../theme';
import { Archive, FileText, Search, ChevronDown, ChevronRight, Download, Loader, Image, Film, FileArchive, File, Binary, Code, ScanLine, ChevronLeft, ChevronsLeft, ChevronsRight } from 'lucide-react';

const BASE = '/api';

// ---- Filetype detection ----

const imageExts = new Set(['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.ico']);
const videoExts = new Set(['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm']);
const archiveExts = new Set(['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2', '.xz', '.tgz']);
const docExts = new Set(['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.odt']);

type FileCategory = 'image' | 'video' | 'archive' | 'document' | 'text' | 'code' | 'binary' | 'unknown';

const codeExts = new Set(['.py', '.js', '.ts', '.jsx', '.tsx', '.go', '.rs', '.c', '.cpp', '.h', '.java', '.rb', '.php', '.sh', '.bash', '.ps1', '.bat']);

function getFileCategory(filename: string, isText?: boolean, mimeType?: string): FileCategory {
  const ext = ('.' + (filename.split('.').pop() || '')).toLowerCase();
  // Use MIME type if available
  if (mimeType) {
    if (mimeType.startsWith('image/')) return 'image';
    if (mimeType.startsWith('video/')) return 'video';
    if (mimeType.startsWith('audio/')) return 'unknown';
    if (mimeType === 'application/zip' || mimeType === 'application/x-rar-compressed' ||
        mimeType === 'application/gzip' || mimeType === 'application/x-7z-compressed' ||
        mimeType === 'application/x-bzip2' || mimeType === 'application/x-tar') return 'archive';
    if (mimeType === 'application/pdf' || mimeType.includes('officedocument') ||
        mimeType.includes('msword') || mimeType.includes('ms-excel')) return 'document';
  }
  if (imageExts.has(ext)) return 'image';
  if (videoExts.has(ext)) return 'video';
  if (archiveExts.has(ext)) return 'archive';
  if (docExts.has(ext)) return 'document';
  if (codeExts.has(ext)) return 'code';
  if (isText || textExts.has(ext)) return 'text';
  return 'unknown';
}

function getMimeLabel(mimeType?: string, category?: FileCategory): string {
  if (mimeType && mimeType !== 'application/octet-stream') {
    // Shorten common MIME types
    const short: Record<string, string> = {
      'application/zip': 'ZIP',
      'application/x-rar-compressed': 'RAR',
      'application/gzip': 'GZ',
      'application/x-7z-compressed': '7Z',
      'application/pdf': 'PDF',
      'image/png': 'PNG',
      'image/jpeg': 'JPEG',
      'image/gif': 'GIF',
      'text/plain': 'Text',
      'text/csv': 'CSV',
      'text/html': 'HTML',
      'application/json': 'JSON',
      'application/sql': 'SQL',
    };
    return short[mimeType] || mimeType.split('/').pop() || '';
  }
  return category || '';
}

function FileCategoryIcon({ category, size = 13 }: { category: FileCategory; size?: number }) {
  const iconColors: Record<FileCategory, string> = {
    image: '#a78bfa',
    video: '#f472b6',
    archive: '#fb923c',
    document: '#60a5fa',
    text: colors.accent,
    code: '#34d399',
    binary: '#94a3b8',
    unknown: colors.textDim,
  };
  const c = iconColors[category];
  switch (category) {
    case 'image': return <Image size={size} color={c} />;
    case 'video': return <Film size={size} color={c} />;
    case 'archive': return <FileArchive size={size} color={c} />;
    case 'document': return <FileText size={size} color={c} />;
    case 'text': return <FileText size={size} color={c} />;
    case 'code': return <Code size={size} color={c} />;
    case 'binary': return <Binary size={size} color={c} />;
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

// ---- Hex dump viewer ----

function HexDumpViewer({ s3Key, filename }: { s3Key: string; filename: string }) {
  const [hexData, setHexData] = useState<{ hex_dump: string; mime_type: string; total_size: number; dump_size: number } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    setLoading(true);
    setError(false);
    authedFetch(`${BASE}/hex-dump?s3_key=${encodeURIComponent(s3Key)}&limit=4096`)
      .then(r => {
        if (!r.ok) throw new Error(`${r.status}`);
        return r.json();
      })
      .then(data => { setHexData(data); setLoading(false); })
      .catch(() => { setError(true); setLoading(false); });
  }, [s3Key]);

  if (loading) return <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>Loading hex dump...</div>;
  if (error || !hexData) return <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>Failed to load hex dump</div>;

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8, fontSize: 11, color: colors.textMuted }}>
        <Binary size={12} />
        <span>MIME: {hexData.mime_type}</span>
        <span>|</span>
        <span>Showing {formatSize(hexData.dump_size)} of {formatSize(hexData.total_size)}</span>
      </div>
      <pre style={{
        fontFamily: font.mono, fontSize: 11, lineHeight: 1.6,
        color: '#93c5fd', whiteSpace: 'pre', overflowX: 'auto',
        background: '#0c0c14', padding: 12, borderRadius: 4,
        border: `1px solid ${colors.border}`,
        maxHeight: 500, overflow: 'auto', margin: 0,
      }}>{hexData.hex_dump}</pre>
    </div>
  );
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
  const cat = getFileCategory(archive.file_name, false, archive.file_mime || undefined);
  const isImage = cat === 'image';
  const isText = cat === 'text' || cat === 'code';
  const isBinary = !isImage && !isText;
  const fileUrl = `${BASE}/mentions/${archive.mention_id}/file`;
  const downloadUrl = authMentionFileUrl(archive.mention_id);
  const mimeLabel = getMimeLabel(archive.file_mime || undefined, cat);
  // For hex dump we need an s3_key — extract it from the mention metadata
  const [s3Key, setS3Key] = useState<string | null>(null);

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

  // Fetch s3_key for hex dump
  useEffect(() => {
    if (!isBinary) return;
    authedFetch(`${BASE}/mentions/${archive.mention_id}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data?.metadata?.s3_key) setS3Key(data.metadata.s3_key);
      })
      .catch(() => {});
  }, [archive.mention_id, isBinary]);

  return (
    <div style={{ ...card, padding: 16 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <FileCategoryIcon category={cat} size={16} />
        <span style={{ fontSize: 14, fontWeight: 600, color: colors.text, fontFamily: font.mono }}>
          {archive.file_name}
        </span>
        {mimeLabel && <span style={{ fontSize: 10, color: colors.textMuted, background: colors.bgSurface, padding: '1px 5px', borderRadius: 3 }}>{mimeLabel}</span>}
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

      {isBinary && s3Key && (
        <HexDumpViewer s3Key={s3Key} filename={archive.file_name} />
      )}

      {isBinary && !s3Key && (
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
  mime_type?: string;
  ocr_text?: string;
  ocr_confidence?: number;
  ocr_engine?: string;
  channel_ref?: string;
  content_snippet?: string;
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
  channel_ref: string;
  content_snippet: string;
  download_url: string;
  has_credentials: boolean;
}

function authedFetch(url: string): Promise<Response> {
  const token = localStorage.getItem('dd_token');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  return fetch(url, { headers });
}

const paginationBtnStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer',
  color: colors.textDim, padding: '4px', borderRadius: 3,
  display: 'inline-flex', alignItems: 'center',
};

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

  // Pagination state
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize] = useState(50);
  const [totalFiles, setTotalFiles] = useState(0);
  const [sortOrder, setSortOrder] = useState<'newest' | 'oldest'>('newest');

  const totalPages = Math.max(1, Math.ceil(totalFiles / pageSize));

  const loadArchives = useCallback(async (page: number, sort: string) => {
    setArchivesLoading(true);
    try {
      const sortParam = sort === 'oldest' ? '&sort=oldest' : '';
      const res = await authedFetch(`${BASE}/archives?page=${page}&page_size=${pageSize}${sortParam}`);
      if (!res.ok) return;
      const data = await res.json();
      const items = (data.items || []).map((a: Record<string, unknown>) => ({
        mention_id: a.mention_id as string,
        file_name: (a.file_name as string) || 'unknown',
        file_size: (a.total_size as number) || 0,
        file_mime: '',
        source_name: (a.source_name as string) || '',
        collected_at: (a.collected_at as string) || '',
        file_count: (a.file_count as number) || 0,
        channel_ref: (a.channel_ref as string) || '',
        content_snippet: (a.content_snippet as string) || '',
        download_url: (a.download_url as string) || '',
        has_credentials: (a.has_credentials as boolean) || false,
      }));
      setArchives(items);
      setTotalFiles(data.total || 0);
    } catch { /* ignore */ }
    setArchivesLoading(false);
  }, [pageSize]);

  // Load extracted files list on mount and when page/sort changes
  useEffect(() => {
    setSelectedArchive(null);
    loadArchives(currentPage, sortOrder);
  }, [currentPage, sortOrder, loadArchives]);

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
    // For search results with preview content, show it immediately
    if (file.preview && file.source === 'extracted_files') {
      setFileContent(prev => ({ ...prev, [file.id]: file.preview }));
      return;
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
              <div style={{ padding: '10px 14px', borderBottom: `1px solid ${colors.border}`, fontSize: 12, fontWeight: 600, color: colors.textDim, textTransform: 'uppercase', letterSpacing: '0.05em', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span>
                  <Archive size={12} style={{ marginRight: 6, verticalAlign: -1 }} />
                  Archives ({totalFiles.toLocaleString()})
                </span>
                <select
                  value={sortOrder}
                  onChange={e => { setSortOrder(e.target.value as 'newest' | 'oldest'); setCurrentPage(1); }}
                  style={{
                    fontSize: 10, padding: '2px 4px', background: colors.bgSurface,
                    border: `1px solid ${colors.border}`, borderRadius: 3,
                    color: colors.textDim, cursor: 'pointer', textTransform: 'none',
                    fontWeight: 400, letterSpacing: 'normal',
                  }}
                >
                  <option value="newest">Newest first</option>
                  <option value="oldest">Oldest first</option>
                </select>
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
                    <div style={{ fontSize: 11, color: colors.textMuted, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                      <span>{formatSize(a.file_size)}</span>
                      {a.file_count > 0 && <span>{a.file_count} file{a.file_count !== 1 ? 's' : ''}</span>}
                      {a.channel_ref && <span title="Channel">{a.channel_ref}</span>}
                      {!a.channel_ref && a.source_name && <span>{a.source_name}</span>}
                      {a.has_credentials && <span style={{ color: '#f59e0b', fontWeight: 600 }}>creds</span>}
                      <span>{a.collected_at ? new Date(a.collected_at).toLocaleDateString() : ''}</span>
                    </div>
                    {a.content_snippet && (
                      <div style={{ fontSize: 10, color: colors.textMuted, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', marginTop: 2, opacity: 0.7 }}>
                        {a.content_snippet}
                      </div>
                    )}
                  </div>
                );
              })}
              {/* Pagination controls */}
              {totalPages > 1 && (
                <div style={{
                  padding: '8px 14px', borderTop: `1px solid ${colors.border}`,
                  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                  fontSize: 11, color: colors.textMuted, position: 'sticky', bottom: 0,
                  background: colors.bgSurface,
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <button
                      onClick={() => setCurrentPage(1)}
                      disabled={currentPage === 1}
                      style={{ ...paginationBtnStyle, opacity: currentPage === 1 ? 0.3 : 1 }}
                      title="First page"
                    >
                      <ChevronsLeft size={12} />
                    </button>
                    <button
                      onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                      disabled={currentPage === 1}
                      style={{ ...paginationBtnStyle, opacity: currentPage === 1 ? 0.3 : 1 }}
                      title="Previous page"
                    >
                      <ChevronLeft size={12} />
                    </button>
                  </div>
                  <span>
                    Page {currentPage} of {totalPages}
                  </span>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <button
                      onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                      disabled={currentPage === totalPages}
                      style={{ ...paginationBtnStyle, opacity: currentPage === totalPages ? 0.3 : 1 }}
                      title="Next page"
                    >
                      <ChevronRight size={12} />
                    </button>
                    <button
                      onClick={() => setCurrentPage(totalPages)}
                      disabled={currentPage === totalPages}
                      style={{ ...paginationBtnStyle, opacity: currentPage === totalPages ? 0.3 : 1 }}
                      title="Last page"
                    >
                      <ChevronsRight size={12} />
                    </button>
                  </div>
                </div>
              )}
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
                {(() => {
                  const arch = archives.find(a => a.mention_id === selectedArchive);
                  return (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                      <span style={{ fontSize: 13, color: colors.textDim }}>
                        {archiveFiles.length} file{archiveFiles.length !== 1 ? 's' : ''}
                      </span>
                      {arch?.channel_ref && <span style={{ fontSize: 11, color: colors.textMuted }}>from {arch.channel_ref}</span>}
                      {arch && (
                        <a href={authMentionFileUrl(arch.mention_id)} download={arch.file_name}
                          style={{ marginLeft: 'auto', display: 'inline-flex', alignItems: 'center', gap: 4, padding: '3px 8px', fontSize: 11, fontWeight: 600, background: colors.accent, color: '#fff', borderRadius: 4, textDecoration: 'none' }}>
                          <Download size={11} /> Download Archive
                        </a>
                      )}
                    </div>
                  );
                })()}
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  {archiveFiles.map(file => {
                    const fid = file.s3_key || file.filename;
                    const isExpanded = expandedFile === fid;
                    const category = getFileCategory(file.filename, file.is_text, file.mime_type);
                    const isText = category === 'text' || category === 'code';
                    const isImage = category === 'image';
                    const isBinary = !isText && !isImage && category !== 'archive';
                    const content = fileContent[fid] || '';
                    const isLoadingContent = contentLoading === fid;
                    const mimeLabel = getMimeLabel(file.mime_type, category);

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
                          {file.ocr_text && (
                            <span title={`OCR: ${file.ocr_text.slice(0, 120)}`} style={{ fontSize: 9, fontWeight: 600, padding: '1px 5px', borderRadius: 3, color: '#a78bfa', background: 'rgba(167, 139, 250, 0.12)', display: 'inline-flex', alignItems: 'center', gap: 2 }}>
                              <ScanLine size={9} /> OCR {file.ocr_confidence != null ? `${(file.ocr_confidence * 100).toFixed(0)}%` : ''}
                            </span>
                          )}
                          {mimeLabel && <span style={{ fontSize: 10, color: colors.textMuted, background: colors.bgSurface, padding: '1px 5px', borderRadius: 3 }}>{mimeLabel}</span>}
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
                            ) : isBinary && file.s3_key ? (
                              <div>
                                <HexDumpViewer s3Key={file.s3_key} filename={file.filename} />
                                <div style={{ marginTop: 8 }}>
                                  <a href={authDownloadUrl(file.s3_key)} download={file.filename}
                                    style={{ color: colors.accent, fontSize: 12, textDecoration: 'none' }}>
                                    <Download size={11} style={{ verticalAlign: -2, marginRight: 4 }} />Download full file
                                  </a>
                                </div>
                              </div>
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
                fontSize: 12, color: colors.textDim, flexWrap: 'wrap',
              }}>
                <Archive size={13} />
                <span style={{ fontFamily: font.mono, color: colors.text, fontWeight: 600 }}>
                  {group.archive}
                </span>
                <span>— {group.files.length} match{group.files.length !== 1 ? 'es' : ''}</span>
                {group.files[0]?.channel_ref && <span style={{ fontSize: 11, color: colors.textMuted }}>from {group.files[0].channel_ref}</span>}
                <a href={authMentionFileUrl(mentionId)} download={group.archive}
                  style={{ marginLeft: 'auto', display: 'inline-flex', alignItems: 'center', gap: 3, fontSize: 11, color: colors.accent, textDecoration: 'none' }}>
                  <Download size={11} /> Archive
                </a>
              </div>
              {group.files[0]?.content_snippet && (
                <div style={{ padding: '4px 16px', fontSize: 11, color: colors.textMuted, borderBottom: `1px solid ${colors.border}`, fontStyle: 'italic' }}>
                  {group.files[0].content_snippet}
                </div>
              )}

              {/* Files in this archive */}
              <div style={{ padding: '4px 8px' }}>
                {group.files.map(file => {
                  const isExpanded = expandedFile === file.id;
                  const category = getFileCategory(file.filename, file.is_text, file.mime_type);
                  const isText = category === 'text' || category === 'code';
                  const isImage = category === 'image';
                  const isBinary = !isText && !isImage && category !== 'archive';
                  const content = fileContent[file.id] || '';
                  const isLoadingContent = contentLoading === file.id;
                  const mimeLabel = getMimeLabel(file.mime_type, category);

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
                        {file.ocr_text && (
                          <span title={`OCR: ${file.ocr_text.slice(0, 120)}`} style={{ fontSize: 9, fontWeight: 600, padding: '1px 5px', borderRadius: 3, color: '#a78bfa', background: 'rgba(167, 139, 250, 0.12)', display: 'inline-flex', alignItems: 'center', gap: 2 }}>
                            <ScanLine size={9} /> OCR {file.ocr_confidence != null ? `${(file.ocr_confidence * 100).toFixed(0)}%` : ''}
                          </span>
                        )}
                        {mimeLabel && <span style={{ fontSize: 10, color: colors.textMuted, background: colors.bgSurface, padding: '1px 5px', borderRadius: 3 }}>{mimeLabel}</span>}
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
                          ) : isBinary && file.s3_key ? (
                            <div>
                              <HexDumpViewer s3Key={file.s3_key} filename={file.filename} />
                              <div style={{ marginTop: 8 }}>
                                <a href={authDownloadUrl(file.s3_key)} download={file.filename}
                                  style={{ color: colors.accent, fontSize: 12, textDecoration: 'none' }}>
                                  <Download size={11} style={{ verticalAlign: -2, marginRight: 4 }} />Download full file
                                </a>
                              </div>
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
