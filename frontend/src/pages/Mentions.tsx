import { useEffect, useState, useCallback, useRef } from 'react';
import { useSearchParams } from 'react-router-dom';
import { fetchMentions, fetchSources, fetchInstitutions, promoteMention, fetchArchiveContents, fetchMentionChannels, fetchMentionFiles, getMentionFileUrl } from '../api';
import type { MentionFilesResponse } from '../api';
import { colors, card, font } from '../theme';
import type { RawMention, Source, Institution, Severity } from '../types';
import ArchiveContents from '../components/ArchiveContents';
import type { ArchiveFile } from '../components/ArchiveContents';
import { MessageSquare, Filter, Search, ChevronDown, ChevronUp, ExternalLink, ArrowRight, X, Check, Download, Eye, Calendar, ScanLine } from 'lucide-react';
import type { CSSProperties } from 'react';
import MultiSelect from '../components/MultiSelect';

const sourceTypeBadge = (type: string): CSSProperties => ({
  fontSize: 10,
  fontWeight: 600,
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
  padding: '2px 8px',
  borderRadius: 4,
  color: colors.accent,
  background: `${colors.accent}1a`,
});

/* ── Sortable column header ────────────────────────────────────────── */
type SortDir = 'asc' | 'desc' | null;

function SortHeader({ label, field, current, dir, onSort, style }: {
  label: string;
  field: string;
  current: string | null;
  dir: SortDir;
  onSort: (field: string, dir: SortDir) => void;
  style?: CSSProperties;
}) {
  const active = current === field;
  const handleClick = () => {
    if (!active) onSort(field, 'asc');
    else if (dir === 'asc') onSort(field, 'desc');
    else onSort(field, null);
  };
  return (
    <span
      onClick={handleClick}
      style={{
        cursor: 'pointer', userSelect: 'none', display: 'inline-flex',
        alignItems: 'center', gap: 4, fontSize: 11, fontWeight: 600,
        textTransform: 'uppercase', letterSpacing: '0.05em',
        color: active ? colors.accent : colors.textMuted,
        ...style,
      }}
    >
      {label}
      {active && dir === 'asc' && <span>&#9650;</span>}
      {active && dir === 'desc' && <span>&#9660;</span>}
    </span>
  );
}

function TextFilePreview({ s3Key }: { s3Key: string }) {
  const [content, setContent] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const token = localStorage.getItem('dd_token');
    const headers: Record<string, string> = {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    fetch(`/api/files/${s3Key}`, { headers })
      .then(r => {
        if (!r.ok) throw new Error('Failed to load');
        return r.text();
      })
      .then(text => { setContent(text.slice(0, 50000)); setLoading(false); })
      .catch(() => { setContent(null); setLoading(false); });
  }, [s3Key]);

  if (loading) return <div style={{ fontSize: 11, color: colors.textMuted, padding: 8 }}>Loading preview...</div>;
  if (content === null) return null;

  return (
    <pre style={{
      fontFamily: font.mono, fontSize: 11, lineHeight: 1.5,
      color: colors.textDim, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
      background: colors.bg, padding: 12, borderRadius: 4,
      border: `1px solid ${colors.border}`, margin: 0,
      maxHeight: 400, overflow: 'auto',
    }}>
      {content}
    </pre>
  );
}

export default function Mentions() {
  const [searchParams] = useSearchParams();
  const targetMentionId = searchParams.get('mention');
  const [mentions, setMentions] = useState<RawMention[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [institutions, setInstitutions] = useState<Institution[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState<string | null>(targetMentionId);
  const [searchQuery, setSearchQuery] = useState('');
  const [sourceFilters, setSourceFilters] = useState<Set<string>>(new Set());
  const [channelFilters, setChannelFilters] = useState<Set<string>>(new Set());
  const [mediaFilters, setMediaFilters] = useState<Set<string>>(new Set());
  const [promotedFilters, setPromotedFilters] = useState<Set<string>>(new Set(targetMentionId ? [] : ['unmatched']));
  const mentionRowRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [sortBy, setSortBy] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>(null);
  const [promoteId, setPromoteId] = useState<string | null>(null);
  const [promoteForm, setPromoteForm] = useState({ institution_id: '', title: '', severity: 'medium' as Severity, summary: '' });
  const [promoting, setPromoting] = useState(false);
  const [archiveFilesMap, setArchiveFilesMap] = useState<Record<string, ArchiveFile[]>>({});
  const [channels, setChannels] = useState<{ channel: string; count: number }[]>([]);
  const [mentionFilesMap, setMentionFilesMap] = useState<Record<string, MentionFilesResponse>>({});
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const PAGE_SIZE = 50;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  const handleSort = (field: string, dir: SortDir) => {
    setSortBy(dir ? field : null);
    setSortDir(dir);
  };

  const loadMentions = useCallback(async () => {
    setLoading(true);
    const params: Record<string, unknown> = { page, page_size: PAGE_SIZE };
    if (sourceFilters.size > 0) params.source_ids = Array.from(sourceFilters).join(',');
    if (channelFilters.size > 0) params.channels = Array.from(channelFilters).join(',');
    if (mediaFilters.size === 1) {
      if (mediaFilters.has('media')) params.has_media = true;
      else if (mediaFilters.has('text')) params.has_media = false;
    }
    if (promotedFilters.size === 1) {
      if (promotedFilters.has('unmatched')) params.promoted = false;
      else if (promotedFilters.has('promoted')) params.promoted = true;
    }
    if (searchQuery.trim()) params.q = searchQuery.trim();
    if (dateFrom) params.date_from = dateFrom;
    if (dateTo) params.date_to = dateTo;
    if (sortBy && sortDir) { params.sort_by = sortBy; params.sort_dir = sortDir; }
    const data = await fetchMentions(params as Parameters<typeof fetchMentions>[0]);
    setMentions(data.items);
    setTotal(data.total);
    setLoading(false);
  }, [page, sourceFilters, channelFilters, mediaFilters, promotedFilters, searchQuery, dateFrom, dateTo, sortBy, sortDir]);

  useEffect(() => {
    loadMentions();
  }, [loadMentions]);

  useEffect(() => {
    fetchSources().then(setSources);
    fetchInstitutions().then(setInstitutions);
    fetchMentionChannels().then(setChannels);
  }, []);

  useEffect(() => {
    if (!expandedId || archiveFilesMap[expandedId] !== undefined) return;
    // Try extracting from local metadata first (already loaded)
    const mention = mentions.find(m => m.id === expandedId);
    const localFiles = (mention?.metadata as Record<string, unknown> | undefined)?.extracted_file_contents;
    if (Array.isArray(localFiles) && localFiles.length > 0) {
      const mapped = localFiles.map((f: Record<string, string>) => ({
        filename: f.filename || '',
        size: (f.content || '').length,
        preview: (f.content || '').slice(0, 500),
        content: f.content || '',
      }));
      setArchiveFilesMap(prev => ({ ...prev, [expandedId]: mapped }));
      return;
    }
    // Fallback to API call
    fetchArchiveContents('mentions', expandedId)
      .then(r => setArchiveFilesMap(prev => ({ ...prev, [expandedId]: r.files })))
      .catch(() => setArchiveFilesMap(prev => ({ ...prev, [expandedId]: [] })));
  }, [expandedId]);

  // Load file info for expanded mentions that have files
  useEffect(() => {
    if (!expandedId || mentionFilesMap[expandedId] !== undefined) return;
    const mention = mentions.find(m => m.id === expandedId);
    const meta = mention?.metadata as Record<string, unknown> | undefined;
    if (!meta?.s3_key && !meta?.file_name) return;
    fetchMentionFiles(expandedId)
      .then(r => setMentionFilesMap(prev => ({ ...prev, [expandedId]: r })))
      .catch(() => {});
  }, [expandedId]);

  // Deep-link: if ?mention=<id> is set, fetch that mention directly if not in current page
  const deepLinkHandled = useRef(false);
  useEffect(() => {
    if (!targetMentionId || deepLinkHandled.current || loading) return;
    const found = mentions.find(m => m.id === targetMentionId);
    if (found) {
      // Mention is in current page — expand and scroll
      deepLinkHandled.current = true;
      setExpandedId(targetMentionId);
      setTimeout(() => {
        mentionRowRefs.current[targetMentionId]?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }, 100);
    } else if (mentions.length > 0) {
      // Mention not found in current page — fetch it directly and prepend
      deepLinkHandled.current = true;
      const token = localStorage.getItem('dd_token');
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;
      fetch(`/api/mentions/${targetMentionId}`, { headers })
        .then(r => r.ok ? r.json() : null)
        .then((m: RawMention | null) => {
          if (m) {
            setMentions(prev => [m, ...prev.filter(x => x.id !== m.id)]);
            setExpandedId(targetMentionId);
            setTimeout(() => {
              mentionRowRefs.current[targetMentionId]?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }, 200);
          }
        })
        .catch(() => {});
    }
  }, [targetMentionId, mentions, loading]);

  const handlePromote = async (mentionId: string) => {
    if (!promoteForm.institution_id || !promoteForm.title) return;
    setPromoting(true);
    await promoteMention(mentionId, {
      institution_id: promoteForm.institution_id,
      title: promoteForm.title,
      severity: promoteForm.severity,
      summary: promoteForm.summary || undefined,
    });
    setPromoteId(null);
    setPromoteForm({ institution_id: '', title: '', severity: 'medium', summary: '' });
    setPromoting(false);
    loadMentions();
  };

  const formatTimestamp = (ts: string) => {
    const d = new Date(ts);
    const now = new Date();
    const diffMs = now.getTime() - d.getTime();
    const diffMin = Math.floor(diffMs / 60000);
    if (diffMin < 60) return `${diffMin}m ago`;
    const diffHr = Math.floor(diffMin / 60);
    if (diffHr < 24) return `${diffHr}h ago`;
    return d.toLocaleDateString();
  };

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 24 }}>
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 700, marginBottom: 4 }}>Raw Mentions</h1>
          <p style={{ fontSize: 13, color: colors.textMuted, margin: 0 }}>
            Browse collected data that hasn't matched any watchterms. Review and promote to findings.
          </p>
        </div>
        <div style={{ fontSize: 13, color: colors.textDim }}>
          {mentions.length} mention{mentions.length !== 1 ? 's' : ''}
        </div>
      </div>

      {/* Filters */}
      <div style={{ ...card, marginBottom: 16, display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
        <Filter size={14} color={colors.textMuted} />

        <div style={{ position: 'relative', flex: '1 1 200px', maxWidth: 300 }}>
          <Search size={14} style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)', color: colors.textMuted }} />
          <input
            type="text"
            placeholder="Search content..."
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            style={{
              width: '100%', padding: '7px 10px 7px 32px', fontSize: 13,
              background: colors.bgSurface, border: `1px solid ${colors.border}`,
              borderRadius: 6, color: colors.text, outline: 'none',
            }}
          />
        </div>

        <MultiSelect
          label="Sources"
          options={sources.map(s => ({ value: s.id, label: s.name }))}
          selected={sourceFilters}
          onChange={setSourceFilters}
        />

        <MultiSelect
          label="Channels"
          options={channels.map(ch => ({ value: ch.channel, label: `${ch.channel} (${ch.count})` }))}
          selected={channelFilters}
          onChange={setChannelFilters}
        />

        <MultiSelect
          label="Type"
          options={[
            { value: 'media', label: 'Has Files' },
            { value: 'text', label: 'Text Only' },
          ]}
          selected={mediaFilters}
          onChange={setMediaFilters}
        />

        <MultiSelect
          label="Status"
          options={[
            { value: 'unmatched', label: 'Unmatched Only' },
            { value: 'promoted', label: 'Promoted' },
          ]}
          selected={promotedFilters}
          onChange={setPromotedFilters}
        />

        {/* Date range */}
        <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
          <Calendar size={14} color={colors.textMuted} />
          <input
            type="date"
            value={dateFrom}
            onChange={e => setDateFrom(e.target.value)}
            placeholder="From"
            style={{
              padding: '6px 8px', fontSize: 12, background: colors.bgSurface,
              border: `1px solid ${dateFrom ? colors.accent : colors.border}`,
              borderRadius: 6, color: colors.text, outline: 'none',
              colorScheme: 'dark',
            }}
          />
          <span style={{ fontSize: 11, color: colors.textMuted }}>to</span>
          <input
            type="date"
            value={dateTo}
            onChange={e => setDateTo(e.target.value)}
            placeholder="To"
            style={{
              padding: '6px 8px', fontSize: 12, background: colors.bgSurface,
              border: `1px solid ${dateTo ? colors.accent : colors.border}`,
              borderRadius: 6, color: colors.text, outline: 'none',
              colorScheme: 'dark',
            }}
          />
        </div>

        {(searchQuery || sourceFilters.size > 0 || channelFilters.size > 0 || mediaFilters.size > 0 || (promotedFilters.size !== 1 || !promotedFilters.has('unmatched')) || dateFrom || dateTo) && (
          <button
            onClick={() => { setSearchQuery(''); setSourceFilters(new Set()); setChannelFilters(new Set()); setMediaFilters(new Set()); setPromotedFilters(new Set(['unmatched'])); setDateFrom(''); setDateTo(''); }}
            style={{
              background: 'none', border: 'none', color: colors.accent,
              fontSize: 12, cursor: 'pointer', padding: '4px 8px',
            }}
          >
            Clear filters
          </button>
        )}
      </div>

      {/* Column sort headers */}
      <div style={{
        ...card, marginBottom: 8, padding: '10px 20px',
        display: 'flex', alignItems: 'center', gap: 12,
      }}>
        <SortHeader label="Type" field="source_id" current={sortBy} dir={sortDir} onSort={handleSort} style={{ minWidth: 70 }} />
        <SortHeader label="Content" field="content" current={sortBy} dir={sortDir} onSort={handleSort} style={{ flex: 1 }} />
        <SortHeader label="Source" field="source_id" current={sortBy} dir={sortDir} onSort={handleSort} style={{ minWidth: 70 }} />
        <SortHeader label="Collected" field="collected_at" current={sortBy} dir={sortDir} onSort={handleSort} style={{ minWidth: 80 }} />
      </div>

      {/* Mentions list */}
      {loading ? (
        <div style={{ color: colors.textMuted, padding: 40, textAlign: 'center' }}>Loading...</div>
      ) : mentions.length === 0 ? (
        <div style={{ ...card, textAlign: 'center', padding: 40, color: colors.textMuted }}>
          <MessageSquare size={32} style={{ marginBottom: 8, opacity: 0.5 }} />
          <div>No mentions found matching your filters.</div>
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {mentions.map(mention => {
            const isExpanded = expandedId === mention.id;
            const isPromoting = promoteId === mention.id;
            const meta = mention.metadata as Record<string, string | number | boolean | null> | undefined;

            return (
              <div key={mention.id} ref={el => { mentionRowRefs.current[mention.id] = el; }} style={{ ...card, padding: 0, overflow: 'hidden', ...(mention.id === targetMentionId ? { border: `1px solid ${colors.accent}`, boxShadow: `0 0 8px ${colors.accent}33` } : {}) }}>
                {/* Row header */}
                <div
                  style={{
                    display: 'flex', alignItems: 'center', gap: 12, padding: '14px 20px',
                    cursor: 'pointer',
                  }}
                  onClick={() => setExpandedId(isExpanded ? null : mention.id)}
                >
                  <span style={sourceTypeBadge(mention.source_type || '')}>
                    {mention.source_type?.replace('_', ' ') || 'unknown'}
                  </span>
                  <span style={{ fontSize: 13, color: colors.textDim, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {mention.content.slice(0, 120)}{mention.content.length > 120 ? '...' : ''}
                  </span>
                  {meta?.channel_ref && (
                    <span style={{ fontSize: 11, color: colors.accent, whiteSpace: 'nowrap', fontFamily: font.mono }}>
                      #{String(meta.channel_ref)}
                    </span>
                  )}
                  {meta?.file_name && (
                    <span style={{ fontSize: 11, whiteSpace: 'nowrap', display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                      <span style={{ color: colors.textDim }}>📎 {String(meta.file_name)}</span>
                      {meta.file_size && (
                        <span style={{ color: colors.textMuted }}>
                          ({Number(meta.file_size) >= 1048576
                            ? (Number(meta.file_size) / 1048576).toFixed(1) + ' MB'
                            : (Number(meta.file_size) / 1024).toFixed(0) + ' KB'})
                        </span>
                      )}
                      {meta.download_status && (
                        <span style={{
                          fontSize: 9, fontWeight: 600, padding: '1px 5px', borderRadius: 3,
                          color: 'white',
                          background: meta.download_status === 'stored' ? colors.healthy
                            : meta.download_status === 'error' ? colors.critical
                            : colors.textMuted,
                        }}>
                          {String(meta.download_status).toUpperCase()}
                        </span>
                      )}
                    </span>
                  )}
                  <span style={{ fontSize: 11, color: colors.textMuted, whiteSpace: 'nowrap' }}>
                    {mention.source_name}
                  </span>
                  <span style={{ fontSize: 11, color: colors.textMuted, whiteSpace: 'nowrap' }}>
                    {formatTimestamp(mention.collected_at)}
                  </span>
                  {meta?.ocr_text && (
                    <span
                      title={`OCR: ${String(meta.ocr_text).slice(0, 200)}`}
                      style={{
                        fontSize: 10, fontWeight: 600, padding: '2px 8px', borderRadius: 4,
                        color: '#a78bfa', background: 'rgba(167, 139, 250, 0.12)',
                        display: 'inline-flex', alignItems: 'center', gap: 3,
                      }}
                    >
                      <ScanLine size={10} /> OCR
                    </span>
                  )}
                  {mention.promoted_to_finding_id && (
                    <span style={{
                      fontSize: 10, fontWeight: 600, padding: '2px 8px', borderRadius: 4,
                      color: colors.healthy, background: colors.healthyBg,
                    }}>
                      PROMOTED
                    </span>
                  )}
                  {isExpanded ? <ChevronUp size={14} color={colors.textMuted} /> : <ChevronDown size={14} color={colors.textMuted} />}
                </div>

                {/* Expanded content */}
                {isExpanded && (
                  <div style={{ borderTop: `1px solid ${colors.border}`, padding: '16px 20px' }}>
                    {/* Metadata context */}
                    {meta && (
                      <div style={{ display: 'flex', gap: 16, marginBottom: 12, flexWrap: 'wrap' }}>
                        {meta.channel_ref && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Channel: </span>
                            <span style={{ color: colors.text }}>{String(meta.channel_ref)}</span>
                          </div>
                        )}
                        {meta.chat_id && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Chat ID: </span>
                            <span style={{ color: colors.text, fontFamily: font.mono }}>{String(meta.chat_id)}</span>
                          </div>
                        )}
                        {meta.forwarded_from && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Forwarded from: </span>
                            <span style={{ color: colors.text }}>{String(meta.forwarded_from)}</span>
                          </div>
                        )}
                        {meta.file_name && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>File: </span>
                            <span style={{ color: colors.text, fontFamily: font.mono }}>{String(meta.file_name)}</span>
                            {meta.file_size && <span style={{ color: colors.textMuted }}> ({(Number(meta.file_size) / 1024).toFixed(0)} KB)</span>}
                          </div>
                        )}
                        {meta.media_type && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Media: </span>
                            <span style={{ color: colors.text }}>{String(meta.media_type)}</span>
                          </div>
                        )}
                        {meta.download_status && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Download: </span>
                            <span style={{ color: meta.download_status === 'stored' ? colors.healthy : meta.download_status === 'error' ? colors.critical : colors.textDim }}>
                              {String(meta.download_status)}
                            </span>
                          </div>
                        )}
                      </div>
                    )}

                    {/* OCR Extracted Text */}
                    {meta?.ocr_text && (
                      <div style={{
                        marginBottom: 12, padding: 12, background: 'rgba(167, 139, 250, 0.06)',
                        borderRadius: 6, border: '1px solid rgba(167, 139, 250, 0.2)',
                      }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
                          <ScanLine size={13} color="#a78bfa" />
                          <span style={{ fontSize: 12, fontWeight: 600, color: '#a78bfa' }}>OCR Extracted Text</span>
                          {meta.ocr_confidence != null && (
                            <span style={{
                              fontSize: 10, color: Number(meta.ocr_confidence) > 0.8 ? colors.healthy : Number(meta.ocr_confidence) > 0.5 ? colors.medium : colors.critical,
                              fontWeight: 600,
                            }}>
                              {(Number(meta.ocr_confidence) * 100).toFixed(0)}% confidence
                            </span>
                          )}
                        </div>
                        <pre style={{
                          fontFamily: font.mono, fontSize: 11, lineHeight: 1.5,
                          color: colors.textDim, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
                          background: colors.bg, padding: 10, borderRadius: 4,
                          border: `1px solid ${colors.border}`, margin: 0,
                          maxHeight: 200, overflow: 'auto',
                        }}>
                          {String(meta.ocr_text)}
                        </pre>
                      </div>
                    )}

                    {/* Full content */}
                    <pre style={{
                      fontFamily: font.mono, fontSize: 12, lineHeight: 1.6,
                      color: colors.textDim, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
                      background: colors.bgSurface, padding: 16, borderRadius: 6,
                      border: `1px solid ${colors.border}`, margin: 0, marginBottom: 12,
                    }}>
                      {mention.content}
                    </pre>

                    {/* Archive Contents (if present) */}
                    {archiveFilesMap[mention.id] && archiveFilesMap[mention.id].length > 0 && (
                      <ArchiveContents files={archiveFilesMap[mention.id]} />
                    )}

                    {/* Single file viewing & download */}
                    {meta?.s3_key && (() => {
                      const fileInfo = mentionFilesMap[mention.id];
                      const mime = meta.file_mime ? String(meta.file_mime) : '';
                      const fileName = meta.file_name ? String(meta.file_name) : 'file';
                      const isImage = mime.startsWith('image/');
                      const isText = mime.startsWith('text/') || ['application/json', 'application/xml', 'application/javascript'].includes(mime)
                        || /\.(txt|log|csv|json|xml|yaml|yml|md|ini|cfg|conf|py|js|ts|sh|sql|html|css)$/i.test(fileName);
                      const downloadUrl = getMentionFileUrl(mention.id);
                      const token = localStorage.getItem('dd_token');
                      const authParam = token ? `?token=${encodeURIComponent(token)}` : '';

                      return (
                        <div style={{
                          marginBottom: 12, padding: 12, background: colors.bgSurface,
                          borderRadius: 6, border: `1px solid ${colors.border}`,
                        }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                            <Eye size={14} color={colors.textMuted} />
                            <span style={{ fontSize: 12, fontWeight: 600, color: colors.text }}>
                              {fileName}
                            </span>
                            {meta.file_size && (
                              <span style={{ fontSize: 11, color: colors.textMuted }}>
                                ({Number(meta.file_size) >= 1048576
                                  ? (Number(meta.file_size) / 1048576).toFixed(1) + ' MB'
                                  : (Number(meta.file_size) / 1024).toFixed(0) + ' KB'})
                              </span>
                            )}
                            <a
                              href={`${downloadUrl}${authParam}`}
                              download={fileName}
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
                            <img
                              src={`/api/mentions/${mention.id}/file`}
                              alt={fileName}
                              style={{
                                maxWidth: '100%', maxHeight: 400, borderRadius: 4,
                                border: `1px solid ${colors.border}`,
                              }}
                            />
                          )}

                          {isText && fileInfo?.files?.length > 0 && (() => {
                            const origFile = fileInfo.files.find(f => f.type === 'original');
                            if (!origFile?.s3_key) return null;
                            return (
                              <TextFilePreview s3Key={origFile.s3_key} />
                            );
                          })()}
                        </div>
                      );
                    })()}

                    {/* Download button for files without s3_key but with file_name */}
                    {meta?.file_name && !meta?.s3_key && meta?.download_status === 'stored' && (
                      <div style={{
                        marginBottom: 12, padding: 12, background: colors.bgSurface,
                        borderRadius: 6, border: `1px solid ${colors.border}`,
                        display: 'flex', alignItems: 'center', gap: 8,
                      }}>
                        <span style={{ fontSize: 12, color: colors.text }}>{String(meta.file_name)}</span>
                        <a
                          href={getMentionFileUrl(mention.id)}
                          download={String(meta.file_name)}
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
                    )}

                    <div style={{ display: 'flex', alignItems: 'center', gap: 12, fontSize: 11, color: colors.textMuted }}>
                      <span>ID: {mention.id}</span>
                      <span>Collected: {new Date(mention.collected_at).toLocaleString()}</span>
                      {mention.source_url && (
                        <a href={mention.source_url} target="_blank" rel="noopener noreferrer"
                          style={{ color: colors.accent, display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                          <ExternalLink size={11} /> Source
                        </a>
                      )}
                    </div>

                    {/* Promote action */}
                    {!mention.promoted_to_finding_id && !isPromoting && (
                      <button
                        onClick={e => { e.stopPropagation(); setPromoteId(mention.id); }}
                        style={{
                          marginTop: 12, padding: '8px 16px', fontSize: 12, fontWeight: 600,
                          background: colors.accent, color: '#fff', border: 'none', borderRadius: 6,
                          cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 6,
                        }}
                      >
                        <ArrowRight size={14} /> Create Finding from Mention
                      </button>
                    )}

                    {mention.promoted_to_finding_id && (
                      <div style={{ marginTop: 12, fontSize: 12, color: colors.healthy }}>
                        Promoted to finding: {mention.promoted_to_finding_id}
                      </div>
                    )}

                    {/* Promote form */}
                    {isPromoting && (
                      <div style={{
                        marginTop: 12, padding: 16, background: colors.bgSurface,
                        borderRadius: 6, border: `1px solid ${colors.border}`,
                      }}>
                        <div style={{ fontSize: 13, fontWeight: 600, color: colors.text, marginBottom: 12 }}>
                          Create Finding from Mention
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                          <div>
                            <label style={{ fontSize: 11, color: colors.textMuted, display: 'block', marginBottom: 4 }}>Institution *</label>
                            <select
                              value={promoteForm.institution_id}
                              onChange={e => setPromoteForm(p => ({ ...p, institution_id: e.target.value }))}
                              style={{
                                width: '100%', padding: '7px 10px', fontSize: 13,
                                background: colors.bg, border: `1px solid ${colors.border}`,
                                borderRadius: 6, color: colors.text, outline: 'none',
                              }}
                            >
                              <option value="">Select institution...</option>
                              {institutions.map(inst => <option key={inst.id} value={inst.id}>{inst.name}</option>)}
                            </select>
                          </div>
                          <div>
                            <label style={{ fontSize: 11, color: colors.textMuted, display: 'block', marginBottom: 4 }}>Title *</label>
                            <input
                              type="text"
                              value={promoteForm.title}
                              onChange={e => setPromoteForm(p => ({ ...p, title: e.target.value }))}
                              placeholder="Finding title..."
                              style={{
                                width: '100%', padding: '7px 10px', fontSize: 13,
                                background: colors.bg, border: `1px solid ${colors.border}`,
                                borderRadius: 6, color: colors.text, outline: 'none',
                              }}
                            />
                          </div>
                          <div style={{ display: 'flex', gap: 10 }}>
                            <div style={{ flex: 1 }}>
                              <label style={{ fontSize: 11, color: colors.textMuted, display: 'block', marginBottom: 4 }}>Severity</label>
                              <select
                                value={promoteForm.severity}
                                onChange={e => setPromoteForm(p => ({ ...p, severity: e.target.value as Severity }))}
                                style={{
                                  width: '100%', padding: '7px 10px', fontSize: 13,
                                  background: colors.bg, border: `1px solid ${colors.border}`,
                                  borderRadius: 6, color: colors.text, outline: 'none',
                                }}
                              >
                                <option value="critical">Critical</option>
                                <option value="high">High</option>
                                <option value="medium">Medium</option>
                                <option value="low">Low</option>
                                <option value="info">Info</option>
                              </select>
                            </div>
                          </div>
                          <div>
                            <label style={{ fontSize: 11, color: colors.textMuted, display: 'block', marginBottom: 4 }}>Summary</label>
                            <textarea
                              value={promoteForm.summary}
                              onChange={e => setPromoteForm(p => ({ ...p, summary: e.target.value }))}
                              placeholder="Optional summary..."
                              rows={2}
                              style={{
                                width: '100%', padding: '7px 10px', fontSize: 13,
                                background: colors.bg, border: `1px solid ${colors.border}`,
                                borderRadius: 6, color: colors.text, outline: 'none',
                                fontFamily: font.sans, resize: 'vertical',
                              }}
                            />
                          </div>
                          <div style={{ display: 'flex', gap: 8 }}>
                            <button
                              onClick={() => handlePromote(mention.id)}
                              disabled={promoting || !promoteForm.institution_id || !promoteForm.title}
                              style={{
                                padding: '8px 16px', fontSize: 12, fontWeight: 600,
                                background: promoteForm.institution_id && promoteForm.title ? colors.accent : colors.bgHover,
                                color: '#fff', border: 'none', borderRadius: 6,
                                cursor: promoteForm.institution_id && promoteForm.title ? 'pointer' : 'not-allowed',
                                display: 'inline-flex', alignItems: 'center', gap: 6,
                                opacity: promoting ? 0.6 : 1,
                              }}
                            >
                              <Check size={14} /> {promoting ? 'Creating...' : 'Create Finding'}
                            </button>
                            <button
                              onClick={() => { setPromoteId(null); setPromoteForm({ institution_id: '', title: '', severity: 'medium', summary: '' }); }}
                              style={{
                                padding: '8px 16px', fontSize: 12,
                                background: 'none', color: colors.textMuted, border: `1px solid ${colors.border}`,
                                borderRadius: 6, cursor: 'pointer',
                                display: 'inline-flex', alignItems: 'center', gap: 6,
                              }}
                            >
                              <X size={14} /> Cancel
                            </button>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', gap: 12, marginTop: 16 }}>
          <button
            onClick={() => setPage(p => Math.max(1, p - 1))}
            disabled={page <= 1}
            style={{ padding: '6px 14px', fontSize: 13, background: colors.bgSurface, border: `1px solid ${colors.border}`, borderRadius: 6, color: page <= 1 ? colors.textMuted : colors.text, cursor: page <= 1 ? 'default' : 'pointer' }}
          >
            Previous
          </button>
          <span style={{ fontSize: 13, color: colors.textDim }}>
            Page {page} of {totalPages} ({total} total)
          </span>
          <button
            onClick={() => setPage(p => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages}
            style={{ padding: '6px 14px', fontSize: 13, background: colors.bgSurface, border: `1px solid ${colors.border}`, borderRadius: 6, color: page >= totalPages ? colors.textMuted : colors.text, cursor: page >= totalPages ? 'default' : 'pointer' }}
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}
