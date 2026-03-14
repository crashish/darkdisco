import { useState, useMemo } from 'react';
import { colors, card, font } from '../theme';
import { Archive, File, Search, ChevronDown, ChevronRight, FileText } from 'lucide-react';
import type { CSSProperties } from 'react';

export interface ArchiveFile {
  filename: string;
  size: number;
  preview: string;
  content: string;
}

interface Props {
  files: ArchiveFile[];
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

export default function ArchiveContents({ files }: Props) {
  const [search, setSearch] = useState('');
  const [expandedFile, setExpandedFile] = useState<string | null>(null);

  const filtered = useMemo(() => {
    if (!search.trim()) return files;
    const needle = search.toLowerCase();
    return files.filter(
      f => f.filename.toLowerCase().includes(needle) || f.content.toLowerCase().includes(needle)
    );
  }, [files, search]);

  if (files.length === 0) return null;

  return (
    <div style={containerStyle}>
      <div style={headerStyle}>
        <Archive size={14} /> Archive Contents ({files.length} file{files.length !== 1 ? 's' : ''})
      </div>

      {/* Search */}
      <div style={{ position: 'relative', marginBottom: 12 }}>
        <Search size={14} style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)', color: colors.textMuted }} />
        <input
          type="text"
          placeholder="Search filenames and content..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={searchInputStyle}
        />
      </div>

      {/* File count summary when filtering */}
      {search.trim() && (
        <div style={{ fontSize: 11, color: colors.textMuted, marginBottom: 8 }}>
          {filtered.length} of {files.length} file{files.length !== 1 ? 's' : ''} match{filtered.length === 1 ? 'es' : ''}
        </div>
      )}

      {/* File listing */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {filtered.map(file => {
          const isExpanded = expandedFile === file.filename;
          return (
            <div key={file.filename}>
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
                <FileText size={14} color={colors.accent} />
                <span style={{ fontSize: 13, color: colors.text, fontFamily: font.mono, flex: 1 }}>
                  {search.trim() ? highlightMatches(file.filename, search.trim()) : file.filename}
                </span>
                <span style={{ fontSize: 11, color: colors.textMuted, whiteSpace: 'nowrap' }}>
                  {formatSize(file.size)}
                </span>
              </div>

              {/* Inline preview */}
              {isExpanded && (
                <pre style={previewStyle}>
                  {search.trim()
                    ? highlightMatches(file.content, search.trim())
                    : file.content
                  }
                </pre>
              )}
            </div>
          );
        })}

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
