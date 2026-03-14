import { useEffect, useState, useCallback } from 'react';
import { fetchMentions, fetchSources, fetchInstitutions, promoteMention, fetchArchiveContents } from '../api';
import { colors, card, font } from '../theme';
import type { RawMention, Source, Institution, Severity } from '../types';
import ArchiveContents from '../components/ArchiveContents';
import type { ArchiveFile } from '../components/ArchiveContents';
import { MessageSquare, Filter, Search, ChevronDown, ChevronUp, ExternalLink, ArrowRight, X, Check } from 'lucide-react';
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

export default function Mentions() {
  const [mentions, setMentions] = useState<RawMention[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [institutions, setInstitutions] = useState<Institution[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [sourceFilter, setSourceFilter] = useState('');
  const [channelFilter, setChannelFilter] = useState('');
  const [mediaFilter, setMediaFilter] = useState('');
  const [promotedFilter, setPromotedFilter] = useState<string>('unmatched');
  const [promoteId, setPromoteId] = useState<string | null>(null);
  const [promoteForm, setPromoteForm] = useState({ institution_id: '', title: '', severity: 'medium' as Severity, summary: '' });
  const [promoting, setPromoting] = useState(false);
  const [archiveFilesMap, setArchiveFilesMap] = useState<Record<string, ArchiveFile[]>>({});
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const PAGE_SIZE = 50;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

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
  }, [page, sourceFilter, channelFilter, mediaFilter, promotedFilter, searchQuery]);

  useEffect(() => {
    loadMentions();
  }, [loadMentions]);

  useEffect(() => {
    fetchSources().then(setSources);
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

        <select
          value={sourceFilter}
          onChange={e => setSourceFilter(e.target.value)}
          style={{
            padding: '7px 10px', fontSize: 13, background: colors.bgSurface,
            border: `1px solid ${colors.border}`, borderRadius: 6, color: colors.text, outline: 'none',
          }}
        >
          <option value="">All Sources</option>
          {sources.map(s => <option key={s.id} value={s.id}>{s.name}</option>)}
        </select>

        <input
          type="text"
          placeholder="Filter by channel..."
          value={channelFilter}
          onChange={e => setChannelFilter(e.target.value)}
          style={{
            padding: '7px 10px', fontSize: 13, background: colors.bgSurface,
            border: `1px solid ${colors.border}`, borderRadius: 6, color: colors.text, outline: 'none',
            width: 160,
          }}
        />

        <select
          value={mediaFilter}
          onChange={e => setMediaFilter(e.target.value)}
          style={{
            padding: '7px 10px', fontSize: 13, background: colors.bgSurface,
            border: `1px solid ${colors.border}`, borderRadius: 6, color: colors.text, outline: 'none',
          }}
        >
          <option value="">All Types</option>
          <option value="media">Has Files</option>
          <option value="text">Text Only</option>
        </select>

        <select
          value={promotedFilter}
          onChange={e => setPromotedFilter(e.target.value)}
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
            onClick={() => { setSearchQuery(''); setSourceFilter(''); setChannelFilter(''); setMediaFilter(''); setPromotedFilter('unmatched'); }}
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
