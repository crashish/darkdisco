import { useState, useMemo, useEffect } from 'react';
import { colors, card, font } from '../theme';
import { Archive, File, Search, ChevronDown, ChevronRight, FileText, Folder, FolderOpen, AlertTriangle, Loader } from 'lucide-react';
import type { CSSProperties } from 'react';

export interface ArchiveFile {
  id?: string;
  filename: string;
  size: number;
  preview: string;
  content: string;
  extension?: string | null;
  sha256?: string | null;
  is_text?: boolean;
  s3_key?: string | null;
}

interface Props {
  files: ArchiveFile[];
  onServerSearch?: (query: string) => Promise<ArchiveFile[]>;
}

// Credential patterns to flag in file content
const CREDENTIAL_PATTERNS = [
  /password\s*[:=]\s*\S+/i,
  /passwd\s*[:=]\s*\S+/i,
  /api[_-]?key\s*[:=]\s*\S+/i,
  /secret[_-]?key\s*[:=]\s*\S+/i,
  /access[_-]?token\s*[:=]\s*\S+/i,
  /auth[_-]?token\s*[:=]\s*\S+/i,
  /bearer\s+\S+/i,
  /private[_-]?key/i,
  /-----BEGIN\s+(RSA\s+)?PRIVATE\s+KEY-----/,
  /jdbc:\w+:\/\//i,
  /mongodb(\+srv)?:\/\//i,
  /smtp[_-]?pass/i,
  /aws[_-]?secret/i,
];

function hasCredentialIndicators(content: string): boolean {
  if (!content) return false;
  return CREDENTIAL_PATTERNS.some(p => p.test(content));
}

const containerStyle: CSSProperties = {
  ...card,
  marginBottom: 16,
};

const headerStyle: CSSProperties = {
  fontSize: 13,
  fontWeight: 600,
  color: colors.textDim,
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
  marginBottom: 12,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
};

const searchInputStyle: CSSProperties = {
  width: '100%',
  padding: '7px 10px 7px 32px',
  fontSize: 13,
  background: colors.bgSurface,
  border: `1px solid ${colors.border}`,
  borderRadius: 6,
  color: colors.text,
  outline: 'none',
  fontFamily: font.sans,
};

const fileRowStyle: CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 10,
  padding: '10px 14px',
  background: colors.bgSurface,
  borderRadius: 6,
  border: `1px solid ${colors.border}`,
  cursor: 'pointer',
  transition: 'background 0.15s',
};

const previewStyle: CSSProperties = {
  fontFamily: font.mono,
  fontSize: 12,
  lineHeight: 1.6,
  color: colors.textDim,
  whiteSpace: 'pre-wrap',
  wordBreak: 'break-word',
  background: colors.bg,
  padding: 14,
  borderRadius: 6,
  border: `1px solid ${colors.border}`,
  margin: '8px 0 0 24px',
  maxHeight: 400,
  overflow: 'auto',
};

const metaTagStyle: CSSProperties = {
  fontSize: 10,
  fontFamily: font.mono,
  color: colors.textMuted,
  padding: '1px 6px',
  background: colors.bg,
  borderRadius: 3,
  border: `1px solid ${colors.border}`,
};

function highlightMatches(text: string, search: string): (string | JSX.Element)[] {
  if (!search) return [text];
  const parts: (string | JSX.Element)[] = [];
  const lower = text.toLowerCase();
  const needle = search.toLowerCase();
  let lastIdx = 0;
  let idx = lower.indexOf(needle, lastIdx);
  let key = 0;
  while (idx !== -1) {
    if (idx > lastIdx) parts.push(text.slice(lastIdx, idx));
    parts.push(
      <span key={key++} style={{ background: 'rgba(234, 179, 8, 0.3)', color: colors.text, borderRadius: 2, padding: '0 1px' }}>
        {text.slice(idx, idx + needle.length)}
      </span>
    );
    lastIdx = idx + needle.length;
    idx = lower.indexOf(needle, lastIdx);
  }
  if (lastIdx < text.length) parts.push(text.slice(lastIdx));
  return parts;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

// Build a tree structure from flat file paths
interface TreeNode {
  name: string;
  path: string;
  file?: ArchiveFile;
  children: Map<string, TreeNode>;
}

function buildFileTree(files: ArchiveFile[]): TreeNode {
  const root: TreeNode = { name: '', path: '', children: new Map() };
  for (const file of files) {
    const parts = file.filename.split('/').filter(Boolean);
    let current = root;
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      if (!current.children.has(part)) {
        const path = parts.slice(0, i + 1).join('/');
        current.children.set(part, { name: part, path, children: new Map() });
      }
      current = current.children.get(part)!;
      if (i === parts.length - 1) {
        current.file = file;
      }
    }
  }
  return root;
}

function FileTreeNode({
  node,
  depth,
  expandedFile,
  setExpandedFile,
  search,
}: {
  node: TreeNode;
  depth: number;
  expandedFile: string | null;
  setExpandedFile: (f: string | null) => void;
  search: string;
}) {
  const [folderOpen, setFolderOpen] = useState(true);
  const isFolder = !node.file && node.children.size > 0;
  const isFile = !!node.file;
  const isExpanded = isFile && expandedFile === node.file!.filename;
  const hasCreds = isFile && hasCredentialIndicators(node.file!.content);

  if (isFolder) {
    const children = Array.from(node.children.values()).sort((a, b) => {
      // Folders first, then files
      const aIsFolder = !a.file && a.children.size > 0;
      const bIsFolder = !b.file && b.children.size > 0;
      if (aIsFolder && !bIsFolder) return -1;
      if (!aIsFolder && bIsFolder) return 1;
      return a.name.localeCompare(b.name);
    });

    return (
      <div style={{ marginLeft: depth > 0 ? 16 : 0 }}>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            padding: '4px 8px',
            cursor: 'pointer',
            borderRadius: 4,
            fontSize: 12,
            color: colors.textDim,
          }}
          onClick={() => setFolderOpen(!folderOpen)}
          onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = colors.bgHover; }}
          onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = 'transparent'; }}
        >
          {folderOpen
            ? <><ChevronDown size={12} color={colors.textMuted} /><FolderOpen size={14} color={colors.medium} /></>
            : <><ChevronRight size={12} color={colors.textMuted} /><Folder size={14} color={colors.medium} /></>
          }
          <span style={{ fontFamily: font.mono }}>{node.name}/</span>
          <span style={{ fontSize: 10, color: colors.textMuted }}>
            ({countFiles(node)} file{countFiles(node) !== 1 ? 's' : ''})
          </span>
        </div>
        {folderOpen && children.map(child => (
          <FileTreeNode
            key={child.path}
            node={child}
            depth={depth + 1}
            expandedFile={expandedFile}
            setExpandedFile={setExpandedFile}
            search={search}
          />
        ))}
      </div>
    );
  }

  if (!isFile) return null;
  const file = node.file!;

  return (
    <div style={{ marginLeft: depth > 0 ? 16 : 0, marginBottom: 4 }}>
      <div
        style={{
          ...fileRowStyle,
          padding: '8px 12px',
          background: isExpanded ? colors.bgHover : colors.bgSurface,
        }}
        onClick={() => setExpandedFile(isExpanded ? null : file.filename)}
        onMouseEnter={e => {
          if (!isExpanded) (e.currentTarget as HTMLElement).style.background = colors.bgHover;
        }}
        onMouseLeave={e => {
          if (!isExpanded) (e.currentTarget as HTMLElement).style.background = colors.bgSurface;
        }}
      >
        {isExpanded
          ? <ChevronDown size={12} color={colors.textMuted} />
          : <ChevronRight size={12} color={colors.textMuted} />
        }
        <FileText size={14} color={file.is_text === false ? colors.textMuted : colors.accent} />
        <span style={{ fontSize: 12, color: colors.text, fontFamily: font.mono, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {search.trim() ? highlightMatches(node.name, search.trim()) : node.name}
        </span>
        {hasCreds && (
          <span
            title="Potential credentials detected"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 3,
              fontSize: 10,
              fontWeight: 600,
              color: colors.high,
              background: colors.highBg,
              padding: '1px 6px',
              borderRadius: 3,
            }}
          >
            <AlertTriangle size={10} /> CREDS
          </span>
        )}
        {file.extension && <span style={metaTagStyle}>{file.extension}</span>}
        <span style={{ fontSize: 11, color: colors.textMuted, whiteSpace: 'nowrap' }}>
          {formatSize(file.size)}
        </span>
      </div>

      {isExpanded && (
        <div style={{ marginLeft: 24, marginTop: 6 }}>
          {/* File metadata bar */}
          {(file.sha256 || file.extension) && (
            <div style={{
              display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 6,
              fontSize: 10, color: colors.textMuted, fontFamily: font.mono,
            }}>
              {file.sha256 && (
                <span title={file.sha256}>SHA256: {file.sha256.slice(0, 16)}...</span>
              )}
              {file.extension && <span>ext: {file.extension}</span>}
              <span>{formatSize(file.size)}</span>
              {file.is_text !== undefined && (
                <span>{file.is_text ? 'text' : 'binary'}</span>
              )}
            </div>
          )}
          {file.is_text === false ? (
            <div style={{
              ...previewStyle,
              margin: 0,
              textAlign: 'center',
              color: colors.textMuted,
              padding: 20,
            }}>
              Binary file — no text preview available
            </div>
          ) : (
            <pre style={{ ...previewStyle, margin: 0 }}>
              {search.trim()
                ? highlightMatches(file.content, search.trim())
                : file.content
              }
            </pre>
          )}
        </div>
      )}
    </div>
  );
}

function countFiles(node: TreeNode): number {
  if (node.file) return 1;
  let count = 0;
  for (const child of node.children.values()) {
    count += countFiles(child);
  }
  return count;
}

export default function ArchiveContents({ files, onServerSearch }: Props) {
  const [search, setSearch] = useState('');
  const [expandedFile, setExpandedFile] = useState<string | null>(null);
  const [serverResults, setServerResults] = useState<ArchiveFile[] | null>(null);
  const [searching, setSearching] = useState(false);
  const [useTree, setUseTree] = useState(true);

  // Determine if files have directory structure
  const hasDirectories = useMemo(() => files.some(f => f.filename.includes('/')), [files]);

  // Client-side filtering
  const filtered = useMemo(() => {
    if (serverResults) return serverResults;
    if (!search.trim()) return files;
    const needle = search.toLowerCase();
    return files.filter(
      f => f.filename.toLowerCase().includes(needle) || f.content.toLowerCase().includes(needle)
    );
  }, [files, search, serverResults]);

  // Server-side search with debounce
  useEffect(() => {
    if (!search.trim() || !onServerSearch) {
      setServerResults(null);
      return;
    }
    const timer = setTimeout(async () => {
      setSearching(true);
      try {
        const results = await onServerSearch(search.trim());
        setServerResults(results);
      } catch {
        setServerResults(null);
      }
      setSearching(false);
    }, 400);
    return () => clearTimeout(timer);
  }, [search, onServerSearch]);

  // Build tree from filtered files
  const tree = useMemo(() => buildFileTree(filtered), [filtered]);

  // Count files with credential indicators
  const credFileCount = useMemo(() => files.filter(f => hasCredentialIndicators(f.content)).length, [files]);

  if (files.length === 0) return null;

  // Determine if we should show tree or flat view
  const showTree = useTree && hasDirectories && !search.trim();

  return (
    <div style={containerStyle}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
        <div style={headerStyle}>
          <Archive size={14} /> Archive Contents ({files.length} file{files.length !== 1 ? 's' : ''})
          {credFileCount > 0 && (
            <span style={{
              display: 'inline-flex', alignItems: 'center', gap: 3,
              fontSize: 10, fontWeight: 600, color: colors.high,
              background: colors.highBg, padding: '2px 8px', borderRadius: 4,
              marginLeft: 4,
            }}>
              <AlertTriangle size={10} /> {credFileCount} with credentials
            </span>
          )}
        </div>
        {hasDirectories && (
          <button
            onClick={() => setUseTree(!useTree)}
            style={{
              background: 'none', border: `1px solid ${colors.border}`,
              color: colors.textMuted, fontSize: 11, padding: '3px 8px',
              borderRadius: 4, cursor: 'pointer',
            }}
          >
            {useTree ? 'Flat view' : 'Tree view'}
          </button>
        )}
      </div>

      {/* Search */}
      <div style={{ position: 'relative', marginBottom: 12 }}>
        {searching
          ? <Loader size={14} style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)', color: colors.accent, animation: 'spin 1s linear infinite' }} />
          : <Search size={14} style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)', color: colors.textMuted }} />
        }
        <input
          type="text"
          placeholder="Search filenames and content..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={searchInputStyle}
        />
      </div>

      {/* Filter count */}
      {search.trim() && (
        <div style={{ fontSize: 11, color: colors.textMuted, marginBottom: 8, display: 'flex', alignItems: 'center', gap: 8 }}>
          {filtered.length} of {files.length} file{files.length !== 1 ? 's' : ''} match{filtered.length === 1 ? 'es' : ''}
          {serverResults && <span style={{ color: colors.accent, fontSize: 10 }}>server search</span>}
        </div>
      )}

      {/* File listing */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
        {showTree ? (
          // Tree view
          Array.from(tree.children.values())
            .sort((a, b) => {
              const aIsFolder = !a.file && a.children.size > 0;
              const bIsFolder = !b.file && b.children.size > 0;
              if (aIsFolder && !bIsFolder) return -1;
              if (!aIsFolder && bIsFolder) return 1;
              return a.name.localeCompare(b.name);
            })
            .map(child => (
              <FileTreeNode
                key={child.path}
                node={child}
                depth={0}
                expandedFile={expandedFile}
                setExpandedFile={setExpandedFile}
                search={search}
              />
            ))
        ) : (
          // Flat view
          filtered.map(file => {
            const isExpanded = expandedFile === file.filename;
            const hasCreds = hasCredentialIndicators(file.content);
            return (
              <div key={file.filename} style={{ marginBottom: 4 }}>
                <div
                  style={{
                    ...fileRowStyle,
                    background: isExpanded ? colors.bgHover : colors.bgSurface,
                  }}
                  onClick={() => setExpandedFile(isExpanded ? null : file.filename)}
                  onMouseEnter={e => {
                    if (!isExpanded) (e.currentTarget as HTMLElement).style.background = colors.bgHover;
                  }}
                  onMouseLeave={e => {
                    if (!isExpanded) (e.currentTarget as HTMLElement).style.background = colors.bgSurface;
                  }}
                >
                  {isExpanded
                    ? <ChevronDown size={14} color={colors.textMuted} />
                    : <ChevronRight size={14} color={colors.textMuted} />
                  }
                  <FileText size={14} color={file.is_text === false ? colors.textMuted : colors.accent} />
                  <span style={{ fontSize: 13, color: colors.text, fontFamily: font.mono, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {search.trim() ? highlightMatches(file.filename, search.trim()) : file.filename}
                  </span>
                  {hasCreds && (
                    <span
                      title="Potential credentials detected"
                      style={{
                        display: 'inline-flex', alignItems: 'center', gap: 3,
                        fontSize: 10, fontWeight: 600, color: colors.high,
                        background: colors.highBg, padding: '1px 6px', borderRadius: 3,
                      }}
                    >
                      <AlertTriangle size={10} /> CREDS
                    </span>
                  )}
                  {file.extension && <span style={metaTagStyle}>{file.extension}</span>}
                  <span style={{ fontSize: 11, color: colors.textMuted, whiteSpace: 'nowrap' }}>
                    {formatSize(file.size)}
                  </span>
                </div>

                {/* Inline preview */}
                {isExpanded && (
                  <div style={{ marginLeft: 24, marginTop: 6 }}>
                    {(file.sha256 || file.extension) && (
                      <div style={{
                        display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 6,
                        fontSize: 10, color: colors.textMuted, fontFamily: font.mono,
                      }}>
                        {file.sha256 && (
                          <span title={file.sha256}>SHA256: {file.sha256.slice(0, 16)}...</span>
                        )}
                        {file.extension && <span>ext: {file.extension}</span>}
                        <span>{formatSize(file.size)}</span>
                        {file.is_text !== undefined && (
                          <span>{file.is_text ? 'text' : 'binary'}</span>
                        )}
                      </div>
                    )}
                    {file.is_text === false ? (
                      <div style={{
                        ...previewStyle, margin: 0,
                        textAlign: 'center', color: colors.textMuted, padding: 20,
                      }}>
                        Binary file — no text preview available
                      </div>
                    ) : (
                      <pre style={{ ...previewStyle, margin: 0 }}>
                        {search.trim()
                          ? highlightMatches(file.content, search.trim())
                          : file.content
                        }
                      </pre>
                    )}
                  </div>
                )}
              </div>
            );
          })
        )}

        {filtered.length === 0 && (
          <div style={{ textAlign: 'center', padding: 20, color: colors.textMuted, fontSize: 12 }}>
            <File size={20} style={{ marginBottom: 4, opacity: 0.5 }} />
            <div>No files match &ldquo;{search}&rdquo;</div>
          </div>
        )}
      </div>
    </div>
  );
}
