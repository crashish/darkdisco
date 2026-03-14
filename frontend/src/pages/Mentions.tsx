import { useEffect, useState, useCallback } from 'react';
<<<<<<< Updated upstream
import { fetchMentions, fetchSources, fetchInstitutions, promoteMention, fetchArchiveContents } from '../api';
import { colors, card, font } from '../theme';
import type { RawMention, Source, Institution, Severity } from '../types';
import ArchiveContents from '../components/ArchiveContents';
import type { ArchiveFile } from '../components/ArchiveContents';
import { MessageSquare, Filter, Search, ChevronDown, ChevronUp, ExternalLink, ArrowRight, X, Check } from 'lucide-react';
=======
import { fetchMentions, fetchMentionChannels, fetchMentionFiles, fetchSources, fetchInstitutions, promoteMention } from '../api';
import type { MentionFilesResponse } from '../api';
import { colors, card, font } from '../theme';
import type { RawMention, Source, Institution, Severity } from '../types';
import { MessageSquare, Filter, Search, ChevronDown, ChevronUp, ExternalLink, ArrowRight, X, Check, FileText, Image, Download, Key, Archive, Eye } from 'lucide-react';
>>>>>>> Stashed changes
import type { CSSProperties } from 'react';

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

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function AuthImage({ src, alt, style }: { src: string; alt: string; style?: CSSProperties }) {
  const [blobUrl, setBlobUrl] = useState<string | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const token = localStorage.getItem('dd_token');
    const headers: Record<string, string> = {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    fetch(src, { headers })
      .then(r => { if (!r.ok) throw new Error(); return r.blob(); })
      .then(blob => { if (!cancelled) setBlobUrl(URL.createObjectURL(blob)); })
      .catch(() => { if (!cancelled) setError(true); });
    return () => { cancelled = true; };
  }, [src]);

  if (error) return <span style={{ color: '#ef4444', fontSize: 12 }}>Failed to load image</span>;
  if (!blobUrl) return <span style={{ color: colors.textMuted, fontSize: 12 }}>Loading image...</span>;
  return <img src={blobUrl} alt={alt} style={style} />;
}

export default function Mentions() {
  const [mentions, setMentions] = useState<RawMention[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const PAGE_SIZE = 50;
  const [sources, setSources] = useState<Source[]>([]);
  const [channels, setChannels] = useState<{ channel: string; count: number }[]>([]);
  const [institutions, setInstitutions] = useState<Institution[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [sourceFilter, setSourceFilter] = useState('');
  const [channelFilter, setChannelFilter] = useState('');
  const [mediaFilter, setMediaFilter] = useState('');
  const [promotedFilter, setPromotedFilter] = useState<string>('');
  const [promoteId, setPromoteId] = useState<string | null>(null);
  const [promoteForm, setPromoteForm] = useState({ institution_id: '', title: '', severity: 'medium' as Severity, summary: '' });
  const [promoting, setPromoting] = useState(false);
<<<<<<< Updated upstream
  const [archiveFilesMap, setArchiveFilesMap] = useState<Record<string, ArchiveFile[]>>({});
=======
  const [filesPanelId, setFilesPanelId] = useState<string | null>(null);
  const [filesData, setFilesData] = useState<MentionFilesResponse | null>(null);
  const [filesLoading, setFilesLoading] = useState(false);
  const [imagePreview, setImagePreview] = useState<string | null>(null);

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
>>>>>>> Stashed changes

  const loadMentions = useCallback(async () => {
    setLoading(true);
    const params: Record<string, unknown> = { page, page_size: PAGE_SIZE };
    if (sourceFilter) params.source_id = sourceFilter;
    if (channelFilter) params.channel = channelFilter;
    if (mediaFilter === 'media') params.has_media = true;
    else if (mediaFilter === 'text') params.has_media = false;
    if (promotedFilter === 'unmatched') params.promoted = false;
    else if (promotedFilter === 'promoted') params.promoted = true;
    if (searchQuery.trim()) params.q = searchQuery.trim();
    const data = await fetchMentions(params as Parameters<typeof fetchMentions>[0]);
    setMentions(data.items);
    setTotal(data.total);
    setLoading(false);
  }, [sourceFilter, channelFilter, mediaFilter, promotedFilter, searchQuery, page]);

  useEffect(() => {
    loadMentions();
  }, [loadMentions]);

  useEffect(() => {
    fetchSources().then(setSources);
    fetchMentionChannels().then(setChannels);
    fetchInstitutions().then(setInstitutions);
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

  const loadFiles = async (mentionId: string) => {
    if (filesPanelId === mentionId) {
      setFilesPanelId(null);
      setFilesData(null);
      return;
    }
    setFilesPanelId(mentionId);
    setFilesLoading(true);
    try {
      const data = await fetchMentionFiles(mentionId);
      setFilesData(data);
    } catch { setFilesData(null); }
    setFilesLoading(false);
  };

  const isImageMime = (mime?: string) => mime?.startsWith('image/');


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
          {total} mention{total !== 1 ? 's' : ''}{totalPages > 1 ? ` (page ${page} of ${totalPages})` : ''}
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
            onChange={e => { setSearchQuery(e.target.value); setPage(1); }}
            style={{
              width: '100%', padding: '7px 10px 7px 32px', fontSize: 13,
              background: colors.bgSurface, border: `1px solid ${colors.border}`,
              borderRadius: 6, color: colors.text, outline: 'none',
            }}
          />
        </div>

        <select
          value={sourceFilter}
          onChange={e => { setSourceFilter(e.target.value); setPage(1); }}
          style={{
            padding: '7px 10px', fontSize: 13, background: colors.bgSurface,
            border: `1px solid ${colors.border}`, borderRadius: 6, color: colors.text, outline: 'none',
          }}
        >
          <option value="">All Sources</option>
          {sources.map(s => <option key={s.id} value={s.id}>{s.name}</option>)}
        </select>

        <select
          value={channelFilter}
          onChange={e => { setChannelFilter(e.target.value); setPage(1); }}
          style={{
            padding: '7px 10px', fontSize: 13, background: colors.bgSurface,
            border: `1px solid ${colors.border}`, borderRadius: 6, color: colors.text, outline: 'none',
          }}
        >
          <option value="">All Channels</option>
          {channels.map(ch => <option key={ch.channel} value={ch.channel}>{ch.channel} ({ch.count})</option>)}
        </select>

        <select
          value={mediaFilter}
          onChange={e => { setMediaFilter(e.target.value); setPage(1); }}
          style={{
            padding: '7px 10px', fontSize: 13, background: colors.bgSurface,
            border: `1px solid ${colors.border}`, borderRadius: 6, color: colors.text, outline: 'none',
          }}
        >
          <option value="">All Types</option>
          <option value="media">With Files</option>
          <option value="text">Text Only</option>
        </select>

        <select
          value={promotedFilter}
          onChange={e => { setPromotedFilter(e.target.value); setPage(1); }}
          style={{
            padding: '7px 10px', fontSize: 13, background: colors.bgSurface,
            border: `1px solid ${colors.border}`, borderRadius: 6, color: colors.text, outline: 'none',
          }}
        >
          <option value="unmatched">Unmatched Only</option>
          <option value="promoted">Promoted</option>
          <option value="">All</option>
        </select>

        {(searchQuery || sourceFilter || channelFilter || mediaFilter || promotedFilter !== 'unmatched') && (
          <button
            onClick={() => { setSearchQuery(''); setSourceFilter(''); setChannelFilter(''); setMediaFilter(''); setPromotedFilter('unmatched'); setPage(1); }}
            style={{
              background: 'none', border: 'none', color: colors.accent,
              fontSize: 12, cursor: 'pointer', padding: '4px 8px',
            }}
          >
            Clear filters
          </button>
        )}
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
          {/* Pagination controls */}
          {totalPages > 1 && (
            <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', gap: 12, padding: '8px 0' }}>
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page <= 1}
                style={{
                  padding: '6px 14px', fontSize: 12, fontWeight: 600,
                  background: colors.bgSurface, color: page <= 1 ? colors.textMuted : colors.text,
                  border: `1px solid ${colors.border}`, borderRadius: 6,
                  cursor: page <= 1 ? 'not-allowed' : 'pointer', opacity: page <= 1 ? 0.5 : 1,
                }}
              >
                Prev
              </button>
              <span style={{ fontSize: 13, color: colors.textDim }}>
                Page {page} of {totalPages}
              </span>
              <button
                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                disabled={page >= totalPages}
                style={{
                  padding: '6px 14px', fontSize: 12, fontWeight: 600,
                  background: colors.bgSurface, color: page >= totalPages ? colors.textMuted : colors.text,
                  border: `1px solid ${colors.border}`, borderRadius: 6,
                  cursor: page >= totalPages ? 'not-allowed' : 'pointer', opacity: page >= totalPages ? 0.5 : 1,
                }}
              >
                Next
              </button>
            </div>
          )}
          {mentions.map(mention => {
            const isExpanded = expandedId === mention.id;
            const isPromoting = promoteId === mention.id;
            const meta = mention.metadata as Record<string, string | number | boolean | null> | undefined;

            return (
              <div key={mention.id} style={{ ...card, padding: 0, overflow: 'hidden' }}>
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
                  {meta?.channel_ref && (
                    <span style={{ fontSize: 11, color: colors.text, fontFamily: font.mono, background: colors.bgSurface, padding: '2px 6px', borderRadius: 3, whiteSpace: 'nowrap' }}>
                      {String(meta.channel_ref)}
                    </span>
                  )}
                  <span style={{ fontSize: 13, color: colors.textDim, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {mention.content.slice(0, 120)}{mention.content.length > 120 ? '...' : ''}
                  </span>
                  {meta?.has_media && (
                    <span style={{
                      fontSize: 10, padding: '2px 6px', borderRadius: 3, whiteSpace: 'nowrap',
                      color: meta.s3_key ? '#22c55e' : meta.download_status === 'pending' ? '#f59e0b' : '#eab308',
                      background: meta.s3_key ? '#22c55e1a' : meta.download_status === 'pending' ? '#f59e0b1a' : '#eab3081a',
                    }}>
                      {meta.s3_key ? 'S3' : meta.download_status === 'pending' ? 'DL' : ''}{' '}
                      {meta.file_name ? String(meta.file_name) : String(meta.media_type || 'media')}
                      {meta.file_size ? ` (${formatBytes(Number(meta.file_size))})` : ''}
                    </span>
                  )}
                  <span style={{ fontSize: 11, color: colors.textMuted, whiteSpace: 'nowrap' }}>
                    {mention.source_name}
                  </span>
                  <span style={{ fontSize: 11, color: colors.textMuted, whiteSpace: 'nowrap' }}>
                    {formatTimestamp(mention.collected_at)}
                  </span>
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
                            <span style={{ color: colors.text, fontFamily: font.mono }}>{String(meta.channel_ref)}</span>
                          </div>
                        )}
                        {meta.chat_id && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Chat ID: </span>
                            <span style={{ color: colors.text, fontFamily: font.mono }}>{String(meta.chat_id)}</span>
                          </div>
                        )}
                        {meta.message_id && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Message: </span>
                            <span style={{ color: colors.text }}>#{String(meta.message_id)}</span>
                          </div>
                        )}
                        {meta.forwarded_from && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Forwarded from: </span>
                            <span style={{ color: colors.text }}>{String(meta.forwarded_from)}</span>
                          </div>
                        )}
                        {meta.forum_name && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Forum: </span>
                            <span style={{ color: colors.text }}>{String(meta.forum_name)}</span>
                          </div>
                        )}
                        {meta.post_author && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Author: </span>
                            <span style={{ color: colors.text, fontFamily: font.mono }}>{String(meta.post_author)}</span>
                          </div>
                        )}
                        {meta.file_name && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>File: </span>
                            <span style={{ color: colors.text, fontFamily: font.mono }}>{String(meta.file_name)}</span>
                            {meta.file_size && <span style={{ color: colors.textMuted }}> ({formatBytes(Number(meta.file_size))})</span>}
                            {meta.file_mime && <span style={{ color: colors.textMuted }}> {String(meta.file_mime)}</span>}
                          </div>
                        )}
                        {meta.has_media && !meta.file_name && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: colors.textMuted }}>Media: </span>
                            <span style={{ color: colors.text }}>{String(meta.media_type || 'attached')}</span>
                          </div>
                        )}
                        {meta.s3_key && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: '#22c55e' }}>Stored in S3</span>
                          </div>
                        )}
                        {meta.download_status === 'pending' && !meta.s3_key && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: '#f59e0b' }}>Download pending</span>
                          </div>
                        )}
                        {meta.download_status === 'error' && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: '#ef4444' }}>Download failed{meta.download_error ? `: ${String(meta.download_error)}` : ''}</span>
                          </div>
                        )}
                        {meta.has_credentials && (
                          <div style={{ fontSize: 11 }}>
                            <span style={{ color: '#ef4444', fontWeight: 600 }}>Credentials detected ({String(meta.credential_count)})</span>
                          </div>
                        )}
                      </div>
                    )}

                    {/* Passwords */}
                    {meta?.extracted_passwords && Array.isArray(meta.extracted_passwords) && (meta.extracted_passwords as string[]).length > 0 && (
                      <div style={{
                        display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12,
                        padding: '8px 12px', background: '#f59e0b12', border: '1px solid #f59e0b33',
                        borderRadius: 6,
                      }}>
                        <Key size={14} color="#f59e0b" />
                        <span style={{ fontSize: 12, color: '#f59e0b', fontWeight: 600 }}>Passwords:</span>
                        {(meta.extracted_passwords as string[]).map((pw: string, i: number) => (
                          <code key={i} style={{
                            fontSize: 12, fontFamily: font.mono, color: colors.text,
                            background: colors.bgSurface, padding: '2px 8px', borderRadius: 3,
                            border: `1px solid ${colors.border}`, userSelect: 'all',
                          }}>{pw}</code>
                        ))}
                      </div>
                    )}

                    {/* File actions */}
                    {meta?.has_media && (
                      <div style={{ marginBottom: 12, display: 'flex', gap: 8 }}>
                        <button
                          onClick={(e) => { e.stopPropagation(); loadFiles(mention.id); }}
                          style={{
                            padding: '6px 14px', fontSize: 12, fontWeight: 600,
                            background: filesPanelId === mention.id ? colors.bgHover : colors.bgSurface,
                            color: colors.text, border: `1px solid ${colors.border}`,
                            borderRadius: 6, cursor: 'pointer',
                            display: 'inline-flex', alignItems: 'center', gap: 6,
                          }}
                        >
                          <Archive size={13} />
                          {filesPanelId === mention.id ? 'Hide Files' : 'View Files'}
                        </button>

                        {meta.s3_key && isImageMime(String(meta.file_mime)) && (
                          <button
                            onClick={(e) => { e.stopPropagation(); setImagePreview(imagePreview === mention.id ? null : mention.id); }}
                            style={{
                              padding: '6px 14px', fontSize: 12, fontWeight: 600,
                              background: imagePreview === mention.id ? colors.bgHover : colors.bgSurface,
                              color: colors.text, border: `1px solid ${colors.border}`,
                              borderRadius: 6, cursor: 'pointer',
                              display: 'inline-flex', alignItems: 'center', gap: 6,
                            }}
                          >
                            <Eye size={13} />
                            {imagePreview === mention.id ? 'Hide Image' : 'View Image'}
                          </button>
                        )}

                        {meta.s3_key && (
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              const token = localStorage.getItem('dd_token');
                              const headers: Record<string, string> = {};
                              if (token) headers['Authorization'] = `Bearer ${token}`;
                              fetch(`/api/mentions/${mention.id}/file`, { headers })
                                .then(r => r.blob())
                                .then(blob => {
                                  const url = URL.createObjectURL(blob);
                                  const a = document.createElement('a');
                                  a.href = url;
                                  a.download = String(meta.file_name || 'file');
                                  a.click();
                                  URL.revokeObjectURL(url);
                                });
                            }}
                            style={{
                              padding: '6px 14px', fontSize: 12, fontWeight: 600,
                              background: colors.bgSurface, color: colors.accent,
                              border: `1px solid ${colors.border}`, borderRadius: 6,
                              cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 6,
                            }}
                          >
                            <Download size={13} /> Download
                          </button>
                        )}
                      </div>
                    )}

                    {/* Image preview */}
                    {imagePreview === mention.id && meta?.s3_key && isImageMime(String(meta.file_mime)) && (
                      <div style={{
                        marginBottom: 12, padding: 12, background: colors.bgSurface,
                        borderRadius: 6, border: `1px solid ${colors.border}`, textAlign: 'center',
                      }}>
                        <AuthImage
                          src={`/api/mentions/${mention.id}/file`}
                          alt={String(meta.file_name || 'Image')}
                          style={{ maxWidth: '100%', maxHeight: 500, borderRadius: 4 }}
                        />
                      </div>
                    )}

                    {/* Files panel */}
                    {filesPanelId === mention.id && (
                      <div style={{
                        marginBottom: 12, padding: 16, background: colors.bgSurface,
                        borderRadius: 6, border: `1px solid ${colors.border}`,
                      }}>
                        {filesLoading ? (
                          <div style={{ color: colors.textMuted, fontSize: 12 }}>Loading files...</div>
                        ) : filesData ? (
                          <div>
                            <div style={{ fontSize: 13, fontWeight: 600, color: colors.text, marginBottom: 8 }}>
                              Files
                              {filesData.download_status === 'pending' && (
                                <span style={{ fontSize: 11, fontWeight: 400, color: '#f59e0b', marginLeft: 8 }}>
                                  (download queued)
                                </span>
                              )}
                              {filesData.download_status === 'stored' && (
                                <span style={{ fontSize: 11, fontWeight: 400, color: '#22c55e', marginLeft: 8 }}>
                                  (stored in S3)
                                </span>
                              )}
                            </div>

                            {filesData.passwords.length > 0 && (
                              <div style={{
                                display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10,
                                padding: '6px 10px', background: '#f59e0b12', borderRadius: 4,
                              }}>
                                <Key size={12} color="#f59e0b" />
                                <span style={{ fontSize: 11, color: '#f59e0b' }}>Archive passwords:</span>
                                {filesData.passwords.map((pw, i) => (
                                  <code key={i} style={{
                                    fontSize: 11, fontFamily: font.mono, color: colors.text,
                                    background: colors.bg, padding: '1px 6px', borderRadius: 3,
                                    border: `1px solid ${colors.border}`, userSelect: 'all',
                                  }}>{pw}</code>
                                ))}
                              </div>
                            )}

                            {filesData.has_credentials && (
                              <div style={{
                                display: 'flex', flexDirection: 'column', gap: 4, marginBottom: 10,
                                padding: '6px 10px', background: '#ef44441a', borderRadius: 4,
                              }}>
                                <span style={{ fontSize: 11, color: '#ef4444', fontWeight: 600 }}>
                                  {filesData.credential_count} credential indicators found
                                </span>
                                {filesData.credential_samples.length > 0 && (
                                  <div style={{ fontSize: 10, color: colors.textDim, fontFamily: font.mono }}>
                                    {filesData.credential_samples.slice(0, 5).map((s, i) => (
                                      <div key={i}>{s}</div>
                                    ))}
                                  </div>
                                )}
                              </div>
                            )}

                            {filesData.files.length > 0 ? (
                              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                                {filesData.files.map((f, i) => (
                                  <div key={i}>
                                    <div style={{
                                      display: 'flex', alignItems: 'center', gap: 10, padding: '6px 10px',
                                      background: colors.bg, borderRadius: 4, border: `1px solid ${colors.border}`,
                                    }}>
                                      {isImageMime(f.mime) ? (
                                        <Image size={14} color={colors.accent} />
                                      ) : (
                                        <FileText size={14} color={colors.textMuted} />
                                      )}
                                      <span style={{ fontSize: 12, color: colors.text, fontFamily: font.mono, flex: 1 }}>
                                        {f.filename}
                                      </span>
                                      <span style={{
                                        fontSize: 10, color: f.type === 'extracted' ? colors.accent : colors.textMuted,
                                        textTransform: 'uppercase', fontWeight: 600,
                                      }}>
                                        {f.type}
                                      </span>
                                      {f.size != null && (
                                        <span style={{ fontSize: 11, color: colors.textMuted }}>
                                          {formatBytes(f.size)}
                                        </span>
                                      )}
                                      {f.download_url && (
                                        <>
                                          {isImageMime(f.mime) && (
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation();
                                                setImagePreview(imagePreview === `${mention.id}:${i}` ? null : `${mention.id}:${i}`);
                                              }}
                                              style={{
                                                background: 'none', border: 'none', cursor: 'pointer',
                                                color: colors.accent, padding: '2px 4px',
                                              }}
                                            >
                                              <Eye size={14} />
                                            </button>
                                          )}
                                          <button
                                            onClick={(e) => {
                                              e.stopPropagation();
                                              const token = localStorage.getItem('dd_token');
                                              const hdrs: Record<string, string> = {};
                                              if (token) hdrs['Authorization'] = `Bearer ${token}`;
                                              fetch(`/api${f.download_url}`, { headers: hdrs })
                                                .then(r => r.blob())
                                                .then(blob => {
                                                  const url = URL.createObjectURL(blob);
                                                  const a = document.createElement('a');
                                                  a.href = url;
                                                  a.download = f.filename;
                                                  a.click();
                                                  URL.revokeObjectURL(url);
                                                });
                                            }}
                                            style={{ background: 'none', border: 'none', cursor: 'pointer', color: colors.accent, display: 'flex', padding: '2px 4px' }}
                                          >
                                            <Download size={14} />
                                          </button>
                                        </>
                                      )}
                                    </div>
                                    {/* Inline image preview for individual files */}
                                    {imagePreview === `${mention.id}:${i}` && f.download_url && isImageMime(f.mime) && (
                                      <div style={{
                                        marginTop: 4, padding: 12, background: colors.bg, borderRadius: 6,
                                        border: `1px solid ${colors.border}`, textAlign: 'center',
                                      }}>
                                        <AuthImage
                                          src={`/api${f.download_url}`}
                                          alt={f.filename}
                                          style={{ maxWidth: '100%', maxHeight: 500, borderRadius: 4 }}
                                        />
                                      </div>
                                    )}
                                  </div>
                                ))}
                              </div>
                            ) : (
                              <div style={{ fontSize: 12, color: colors.textMuted }}>
                                {filesData.download_status === 'pending'
                                  ? 'File is queued for download. Check back shortly.'
                                  : 'No files available yet.'}
                              </div>
                            )}
                          </div>
                        ) : (
                          <div style={{ color: colors.textMuted, fontSize: 12 }}>Failed to load file info.</div>
                        )}
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
          {/* Bottom pagination controls */}
          {totalPages > 1 && (
            <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', gap: 12, padding: '8px 0' }}>
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page <= 1}
                style={{
                  padding: '6px 14px', fontSize: 12, fontWeight: 600,
                  background: colors.bgSurface, color: page <= 1 ? colors.textMuted : colors.text,
                  border: `1px solid ${colors.border}`, borderRadius: 6,
                  cursor: page <= 1 ? 'not-allowed' : 'pointer', opacity: page <= 1 ? 0.5 : 1,
                }}
              >
                Prev
              </button>
              <span style={{ fontSize: 13, color: colors.textDim }}>
                Page {page} of {totalPages}
              </span>
              <button
                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                disabled={page >= totalPages}
                style={{
                  padding: '6px 14px', fontSize: 12, fontWeight: 600,
                  background: colors.bgSurface, color: page >= totalPages ? colors.textMuted : colors.text,
                  border: `1px solid ${colors.border}`, borderRadius: 6,
                  cursor: page >= totalPages ? 'not-allowed' : 'pointer', opacity: page >= totalPages ? 0.5 : 1,
                }}
              >
                Next
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
