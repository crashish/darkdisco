import { useState, useEffect, useCallback } from 'react';
import { fetchArchiveContents } from '../api';
import { colors, card, font } from '../theme';
import { Archive, FileText, Search, ChevronDown, ChevronRight, Download, Loader } from 'lucide-react';

const BASE = '/api';

interface ExtractedFile {
  filename: string;
  size: number;
  preview: string;
  content: string;
  s3_key?: string;
  sha256?: string;
  extension?: string;
  is_text?: boolean;
}

interface MentionSummary {
  id: string;
  source_name: string;
  file_name: string;
  file_size: number;
  download_status: string;
  collected_at: string;
  file_count?: number;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

const textExts = new Set(['.txt', '.csv', '.log', '.json', '.xml', '.html', '.sql', '.cfg', '.conf', '.ini', '.env', '.yml', '.yaml', '.py', '.js', '.php']);

export default function Files() {
  const [mentions, setMentions] = useState<MentionSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedMention, setSelectedMention] = useState<string | null>(null);
  const [files, setFiles] = useState<ExtractedFile[]>([]);
  const [filesLoading, setFilesLoading] = useState(false);
  const [expandedFile, setExpandedFile] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState<Record<string, string>>({});
  const [contentLoading, setContentLoading] = useState<string | null>(null);
  const [search, setSearch] = useState('');

  // Load mentions that have files
  useEffect(() => {
    async function load() {
      setLoading(true);
      const token = localStorage.getItem('dd_token');
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;
      try {
        const res = await fetch(`${BASE}/mentions?has_media=true&page_size=200`, { headers });
        if (!res.ok) throw new Error(`${res.status}`);
        const data = await res.json();
        const items = (data.items || [])
          .filter((m: Record<string, unknown>) => {
            const meta = m.metadata as Record<string, unknown> | undefined;
            return meta?.download_status === 'stored';
          })
          .map((m: Record<string, unknown>) => {
            const meta = m.metadata as Record<string, unknown> | undefined;
            return {
              id: m.id as string,
              source_name: m.source_name as string || '',
              file_name: (meta?.file_name as string) || '',
              file_size: (meta?.file_size as number) || 0,
              download_status: (meta?.download_status as string) || '',
              collected_at: m.collected_at as string || '',
            };
          });
        setMentions(items);
      } catch { /* ignore */ }
      setLoading(false);
    }
    load();
  }, []);

  // Load files for selected mention
  const loadFiles = useCallback(async (mentionId: string) => {
    setFilesLoading(true);
    setFiles([]);
    setExpandedFile(null);
    try {
      const data = await fetchArchiveContents('mentions', mentionId);
      setFiles(data.files || []);
    } catch { /* ignore */ }
    setFilesLoading(false);
  }, []);

  useEffect(() => {
    if (selectedMention) loadFiles(selectedMention);
  }, [selectedMention, loadFiles]);

  // Fetch file content on demand from S3
  async function loadFileContent(file: ExtractedFile) {
    if (file.content) {
      setFileContent(prev => ({ ...prev, [file.filename]: file.content }));
      return;
    }
    if (!file.s3_key) return;
    setContentLoading(file.filename);
    try {
      const token = localStorage.getItem('dd_token');
      const headers: Record<string, string> = {};
      if (token) headers['Authorization'] = `Bearer ${token}`;
      const res = await fetch(`${BASE}/files/${file.s3_key}`, { headers });
      if (!res.ok) throw new Error(`${res.status}`);
      const text = await res.text();
      setFileContent(prev => ({ ...prev, [file.filename]: text }));
    } catch {
      setFileContent(prev => ({ ...prev, [file.filename]: '[Failed to load content]' }));
    }
    setContentLoading(null);
  }

  function handleExpandFile(file: ExtractedFile) {
    if (expandedFile === file.filename) {
      setExpandedFile(null);
      return;
    }
    setExpandedFile(file.filename);
    if (!fileContent[file.filename] && (file.is_text || textExts.has(file.extension || ''))) {
      loadFileContent(file);
    }
  }

  const filtered = search.trim()
    ? files.filter(f => f.filename.toLowerCase().includes(search.toLowerCase()))
    : files;

  const selectedInfo = mentions.find(m => m.id === selectedMention);

  return (
    <div>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, marginBottom: 4 }}>File Browser</h1>
        <p style={{ fontSize: 13, color: colors.textMuted, margin: 0 }}>
          Browse and search extracted archive contents from collected mentions.
        </p>
      </div>

      <div style={{ display: 'flex', gap: 16, minHeight: 'calc(100vh - 160px)' }}>
        {/* Left panel: archive list */}
        <div style={{ width: 340, flexShrink: 0 }}>
          <div style={{ ...card, padding: 0, maxHeight: 'calc(100vh - 180px)', overflow: 'auto' }}>
            <div style={{ padding: '12px 16px', borderBottom: `1px solid ${colors.border}`, fontSize: 12, fontWeight: 600, color: colors.textDim, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              <Archive size={12} style={{ marginRight: 6, verticalAlign: -1 }} />
              Archives ({mentions.length})
            </div>
            {loading ? (
              <div style={{ padding: 20, textAlign: 'center', color: colors.textMuted }}>Loading...</div>
            ) : mentions.length === 0 ? (
              <div style={{ padding: 20, textAlign: 'center', color: colors.textMuted, fontSize: 13 }}>No stored archives found</div>
            ) : (
              mentions.map(m => (
                <div
                  key={m.id}
                  onClick={() => setSelectedMention(m.id)}
                  style={{
                    padding: '10px 16px',
                    cursor: 'pointer',
                    borderBottom: `1px solid ${colors.border}`,
                    background: selectedMention === m.id ? colors.bgHover : 'transparent',
                    transition: 'background 0.1s',
                  }}
                  onMouseEnter={e => { if (selectedMention !== m.id) (e.currentTarget as HTMLElement).style.background = colors.bgSurface; }}
                  onMouseLeave={e => { if (selectedMention !== m.id) (e.currentTarget as HTMLElement).style.background = 'transparent'; }}
                >
                  <div style={{ fontSize: 13, fontFamily: font.mono, color: colors.text, marginBottom: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {m.file_name}
                  </div>
                  <div style={{ display: 'flex', gap: 8, fontSize: 11, color: colors.textMuted }}>
                    <span>{formatSize(m.file_size)}</span>
                    <span>{m.source_name}</span>
                    <span>{new Date(m.collected_at).toLocaleDateString()}</span>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Right panel: file contents */}
        <div style={{ flex: 1, minWidth: 0 }}>
          {!selectedMention ? (
            <div style={{ ...card, padding: 40, textAlign: 'center', color: colors.textMuted }}>
              <Archive size={32} style={{ marginBottom: 8, opacity: 0.3 }} />
              <div style={{ fontSize: 14 }}>Select an archive to browse its contents</div>
            </div>
          ) : filesLoading ? (
            <div style={{ ...card, padding: 40, textAlign: 'center', color: colors.textMuted }}>
              <Loader size={20} style={{ animation: 'spin 1s linear infinite', marginBottom: 8 }} />
              <div>Loading files...</div>
            </div>
          ) : (
            <div>
              {/* Header */}
              {selectedInfo && (
                <div style={{ marginBottom: 12, fontSize: 13, color: colors.textDim }}>
                  <span style={{ fontFamily: font.mono, color: colors.text, fontWeight: 600 }}>{selectedInfo.file_name}</span>
                  {' '}&mdash;{' '}{files.length} file{files.length !== 1 ? 's' : ''} extracted
                </div>
              )}

              {/* Search */}
              <div style={{ position: 'relative', marginBottom: 12 }}>
                <Search size={14} style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)', color: colors.textMuted }} />
                <input
                  type="text"
                  placeholder="Filter files by name..."
                  value={search}
                  onChange={e => setSearch(e.target.value)}
                  style={{
                    width: '100%', padding: '8px 10px 8px 32px', fontSize: 13,
                    background: colors.bgSurface, border: `1px solid ${colors.border}`,
                    borderRadius: 6, color: colors.text, outline: 'none',
                  }}
                />
              </div>

              {/* File list */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                {filtered.map(file => {
                  const isExpanded = expandedFile === file.filename;
                  const isText = file.is_text || textExts.has(file.extension || '');
                  const content = fileContent[file.filename] || file.content || '';
                  const isLoadingContent = contentLoading === file.filename;

                  return (
                    <div key={file.s3_key || file.filename}>
                      <div
                        style={{
                          display: 'flex', alignItems: 'center', gap: 8, padding: '8px 12px',
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
                          {file.filename}
                        </span>
                        <span style={{ fontSize: 11, color: colors.textMuted, whiteSpace: 'nowrap' }}>{formatSize(file.size)}</span>
                        {file.s3_key && (
                          <a
                            href={`${BASE}/files/${file.s3_key}`}
                            onClick={e => e.stopPropagation()}
                            style={{ color: colors.textMuted, display: 'flex' }}
                            title="Download"
                          >
                            <Download size={12} />
                          </a>
                        )}
                      </div>

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
                              {content}
                            </pre>
                          ) : isText ? (
                            <div style={{ padding: 12, color: colors.textMuted, fontSize: 12, fontStyle: 'italic' }}>
                              No content available — file may need extraction
                            </div>
                          ) : (
                            <div style={{ padding: 12, color: colors.textMuted, fontSize: 12 }}>
                              Binary file ({file.extension || 'unknown type'}) — {file.s3_key ? 'download to view' : 'not available'}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}

                {filtered.length === 0 && files.length > 0 && (
                  <div style={{ padding: 20, textAlign: 'center', color: colors.textMuted, fontSize: 12 }}>
                    No files match "{search}"
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      <style>{`@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}
