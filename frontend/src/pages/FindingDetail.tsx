import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { fetchFinding, updateFindingStatus, fetchArchiveContents } from '../api';
import { colors, card, font, statusColor } from '../theme';
import SeverityBadge from '../components/SeverityBadge';
import StatusBadge from '../components/StatusBadge';
import ArchiveContents from '../components/ArchiveContents';
import type { ArchiveFile } from '../components/ArchiveContents';
import type { FindingDetail as FindingDetailType, FindingStatus, HighlightSpan } from '../types';
import { ArrowLeft, ExternalLink, Tag, Clock, User, FileText, Shield, Search, ChevronDown, MessageSquare, Forward, Paperclip, Reply, Hash } from 'lucide-react';
import type { CSSProperties } from 'react';

const allStatuses: FindingStatus[] = ['new', 'reviewing', 'confirmed', 'dismissed', 'resolved'];

const sectionStyle: CSSProperties = {
  ...card,
  marginBottom: 16,
};

const sectionTitle: CSSProperties = {
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

const labelStyle: CSSProperties = {
  fontSize: 11,
  color: colors.textMuted,
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
  marginBottom: 4,
};

const valueStyle: CSSProperties = {
  fontSize: 13,
  color: colors.text,
};

function HighlightedContent({ text, matchedTerms }: { text: string; matchedTerms: { term_type: string; highlights?: HighlightSpan[] }[] | null }) {
  // Merge all highlight spans from all matched terms
  const spans: { start: number; end: number }[] = [];
  if (matchedTerms) {
    for (const term of matchedTerms) {
      if (term.highlights) {
        for (const h of term.highlights) {
          spans.push({ start: h.start, end: h.end });
        }
      }
    }
  }

  if (spans.length === 0) return <>{text}</>;

  // Sort by start position, then merge overlapping spans
  spans.sort((a, b) => a.start - b.start);
  const merged: { start: number; end: number }[] = [spans[0]];
  for (let i = 1; i < spans.length; i++) {
    const prev = merged[merged.length - 1];
    if (spans[i].start <= prev.end) {
      prev.end = Math.max(prev.end, spans[i].end);
    } else {
      merged.push({ ...spans[i] });
    }
  }

  // Build segments
  const parts: JSX.Element[] = [];
  let cursor = 0;
  for (let i = 0; i < merged.length; i++) {
    const { start, end } = merged[i];
    const clampedStart = Math.max(0, Math.min(start, text.length));
    const clampedEnd = Math.max(0, Math.min(end, text.length));
    if (cursor < clampedStart) {
      parts.push(<span key={`t${i}`}>{text.slice(cursor, clampedStart)}</span>);
    }
    if (clampedStart < clampedEnd) {
      parts.push(
        <mark key={`h${i}`} style={{
          background: 'rgba(99, 102, 241, 0.25)',
          color: colors.text,
          borderRadius: 2,
          padding: '1px 0',
          borderBottom: `2px solid ${colors.accent}`,
        }}>
          {text.slice(clampedStart, clampedEnd)}
        </mark>
      );
    }
    cursor = clampedEnd;
  }
  if (cursor < text.length) {
    parts.push(<span key="tail">{text.slice(cursor)}</span>);
  }

  return <>{parts}</>;
}

export default function FindingDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [finding, setFinding] = useState<FindingDetailType | null>(null);
  const [loading, setLoading] = useState(true);
  const [statusMenuOpen, setStatusMenuOpen] = useState(false);
  const [archiveFiles, setArchiveFiles] = useState<ArchiveFile[]>([]);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    fetchFinding(id).then(f => {
      setFinding(f);
      setLoading(false);
      // Extract archive files from metadata if available
      const localFiles = (f.metadata as Record<string, unknown> | undefined)?.extracted_file_contents;
      if (Array.isArray(localFiles) && localFiles.length > 0) {
        setArchiveFiles(localFiles.map((ef: Record<string, string>) => ({
          filename: ef.filename || '',
          size: (ef.content || '').length,
          preview: (ef.content || '').slice(0, 500),
          content: ef.content || '',
        })));
      } else {
        // Fallback to API
        fetchArchiveContents('findings', id).then(r => setArchiveFiles(r.files)).catch(() => {});
      }
    });
  }, [id]);

  const handleStatusChange = async (status: FindingStatus) => {
    if (!finding) return;
    await updateFindingStatus(finding.id, status);
    setFinding(prev => prev ? { ...prev, status } : prev);
    setStatusMenuOpen(false);
  };

  if (loading) {
    return <div style={{ color: colors.textMuted, padding: 40, textAlign: 'center' }}>Loading...</div>;
  }

  if (!finding) {
    return <div style={{ color: colors.textMuted, padding: 40, textAlign: 'center' }}>Finding not found.</div>;
  }

  return (
    <div>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <button
          onClick={() => navigate('/findings')}
          style={{
            background: 'none', border: 'none', color: colors.accent,
            fontSize: 13, cursor: 'pointer', padding: 0, marginBottom: 16,
            display: 'inline-flex', alignItems: 'center', gap: 6,
          }}
        >
          <ArrowLeft size={14} /> Back to Findings
        </button>

        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 16 }}>
          <div style={{ flex: 1 }}>
            <h1 style={{ fontSize: 22, fontWeight: 700, marginBottom: 8, lineHeight: 1.3 }}>{finding.title}</h1>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
              <SeverityBadge severity={finding.severity} />
              <div style={{ position: 'relative' }}>
                <div
                  style={{ display: 'inline-flex', alignItems: 'center', gap: 4, cursor: 'pointer' }}
                  onClick={() => setStatusMenuOpen(!statusMenuOpen)}
                >
                  <StatusBadge status={finding.status} />
                  <ChevronDown size={12} color={colors.textMuted} />
                </div>
                {statusMenuOpen && (
                  <div style={{
                    position: 'absolute', top: '100%', left: 0, zIndex: 50, marginTop: 4,
                    background: colors.bgSurface, border: `1px solid ${colors.border}`, borderRadius: 6,
                    padding: 4, minWidth: 140, boxShadow: '0 8px 24px rgba(0,0,0,0.4)',
                  }}>
                    {allStatuses.filter(s => s !== finding.status).map(s => (
                      <div
                        key={s}
                        style={{ padding: '6px 10px', fontSize: 12, cursor: 'pointer', borderRadius: 4, color: colors.textDim }}
                        onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = colors.bgHover; (e.currentTarget as HTMLElement).style.color = colors.text; }}
                        onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = 'transparent'; (e.currentTarget as HTMLElement).style.color = colors.textDim; }}
                        onClick={() => handleStatusChange(s)}
                      >
                        {s.charAt(0).toUpperCase() + s.slice(1)}
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <span style={{ fontSize: 12, color: colors.textMuted }}>{finding.institution_name}</span>
              <span style={{ fontSize: 11, color: colors.textMuted, fontFamily: font.mono }}>{finding.source_type}</span>
              {finding.source_url && (
                <a
                  href={finding.source_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ color: colors.accent, fontSize: 12, display: 'inline-flex', alignItems: 'center', gap: 4 }}
                >
                  <ExternalLink size={12} /> Source
                </a>
              )}
            </div>
            <div style={{ fontSize: 11, color: colors.textMuted, marginTop: 8 }}>
              ID: {finding.id} &middot; Discovered: {new Date(finding.discovered_at).toLocaleString()} &middot; Updated: {new Date(finding.updated_at).toLocaleString()}
              {finding.reviewed_by && <> &middot; Reviewed by: {finding.reviewed_by}</>}
            </div>
          </div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 340px', gap: 16, alignItems: 'start' }}>
        {/* Left column */}
        <div>
          {/* Message Context (for Telegram and other rich sources) */}
          {finding.metadata && Object.keys(finding.metadata).length > 0 && (() => {
            const meta = finding.metadata as Record<string, string | number | boolean | null | Record<string, string>>;
            return (
            <div style={sectionStyle}>
              <div style={sectionTitle}><MessageSquare size={14} /> Message Context</div>
              <div style={{
                background: colors.bgSurface, borderRadius: 6,
                border: `1px solid ${colors.border}`, overflow: 'hidden',
              }}>
                {/* Channel / Forum header */}
                <div style={{
                  padding: '10px 16px', borderBottom: `1px solid ${colors.border}`,
                  display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap',
                }}>
                  {(meta.channel_name || meta.channel_ref) && (
                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, fontSize: 12, color: colors.text, fontWeight: 600 }}>
                      <Hash size={12} color={colors.accent} />
                      {String(meta.channel_name || meta.channel_ref)}
                    </span>
                  )}
                  {meta.forum_name && (
                    <span style={{ fontSize: 12, color: colors.text, fontWeight: 600 }}>
                      {String(meta.forum_name)}
                    </span>
                  )}
                  {finding.source_name && (
                    <span style={{ fontSize: 11, color: colors.textMuted }}>
                      via {finding.source_name}
                    </span>
                  )}
                  {meta.message_date && (
                    <span style={{ fontSize: 11, color: colors.textMuted, marginLeft: 'auto' }}>
                      {new Date(String(meta.message_date)).toLocaleString()}
                    </span>
                  )}
                  {meta.post_date && (
                    <span style={{ fontSize: 11, color: colors.textMuted, marginLeft: 'auto' }}>
                      {new Date(String(meta.post_date)).toLocaleString()}
                    </span>
                  )}
                </div>

                {/* Sender / Author info */}
                {(meta.sender_name || meta.post_author) && (
                  <div style={{ padding: '8px 16px', borderBottom: `1px solid ${colors.border}`, display: 'flex', alignItems: 'center', gap: 8 }}>
                    <User size={12} color={colors.textMuted} />
                    <span style={{ fontSize: 12, color: colors.accent, fontFamily: font.mono }}>
                      {String(meta.sender_name || meta.post_author)}
                    </span>
                  </div>
                )}

                {/* Forwarded-from info */}
                {meta.forwarded_from && (
                  <div style={{
                    padding: '8px 16px', borderBottom: `1px solid ${colors.border}`,
                    display: 'flex', alignItems: 'center', gap: 8, fontSize: 11, color: colors.textMuted,
                  }}>
                    <Forward size={12} />
                    {'Forwarded from: '}<span style={{ color: colors.text }}>
                      {String((meta.forwarded_from as Record<string, string>).channel || 'Unknown channel')}
                    </span>
                  </div>
                )}

                {/* Reply-to info */}
                {meta.reply_to_message_id && (
                  <div style={{
                    padding: '8px 16px', borderBottom: `1px solid ${colors.border}`,
                    display: 'flex', alignItems: 'center', gap: 8, fontSize: 11, color: colors.textMuted,
                  }}>
                    <Reply size={12} />
                    {'Reply to message #'}{String(meta.reply_to_message_id)}
                  </div>
                )}

                {/* Media attachment info */}
                {meta.has_media && (
                  <div style={{
                    padding: '8px 16px', borderBottom: `1px solid ${colors.border}`,
                    display: 'flex', alignItems: 'center', gap: 8, fontSize: 11,
                  }}>
                    <Paperclip size={12} color={colors.textMuted} />
                    <span style={{ color: colors.textMuted }}>Attachment:</span>
                    <span style={{ color: colors.text, fontFamily: font.mono }}>
                      {String(meta.media_filename || meta.media_type || 'file')}
                    </span>
                  </div>
                )}

                {/* Thread info for forums */}
                {meta.thread_title && (
                  <div style={{ padding: '8px 16px', borderBottom: `1px solid ${colors.border}`, fontSize: 11, color: colors.textMuted }}>
                    {'Thread: '}<span style={{ color: colors.text }}>{String(meta.thread_title)}</span>
                  </div>
                )}
              </div>
            </div>
            );
          })()}

          {/* Full Content */}
          {finding.raw_content && (
            <div style={sectionStyle}>
              <div style={sectionTitle}><FileText size={14} /> Full Content</div>
              <pre style={{
                fontFamily: font.mono, fontSize: 12, lineHeight: 1.6,
                color: colors.textDim, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
                background: colors.bgSurface, padding: 16, borderRadius: 6,
                border: `1px solid ${colors.border}`, margin: 0, maxHeight: 600, overflow: 'auto',
              }}>
                <HighlightedContent text={finding.raw_content} matchedTerms={finding.matched_terms} />
              </pre>
              {finding.source_url && (
                <div style={{ marginTop: 8 }}>
                  <a
                    href={finding.source_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ color: colors.accent, fontSize: 12, display: 'inline-flex', alignItems: 'center', gap: 4 }}
                  >
                    <ExternalLink size={12} /> View original source
                  </a>
                </div>
              )}
            </div>
          )}

          {/* Archive Contents */}
          {archiveFiles.length > 0 && <ArchiveContents files={archiveFiles} />}

          {/* Matched Terms */}
          {finding.matched_terms && finding.matched_terms.length > 0 && (
            <div style={sectionStyle}>
              <div style={sectionTitle}><Search size={14} /> Matched Terms</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {finding.matched_terms.map((term, i) => (
                  <div key={i} style={{
                    display: 'flex', alignItems: 'center', gap: 12,
                    padding: '10px 14px', background: colors.bgSurface,
                    borderRadius: 6, border: `1px solid ${colors.border}`,
                  }}>
                    <span style={{
                      fontSize: 10, fontWeight: 600, textTransform: 'uppercase',
                      color: colors.accent, background: `${colors.accent}1a`,
                      padding: '2px 8px', borderRadius: 4, letterSpacing: '0.05em',
                    }}>
                      {term.term_type}
                    </span>
                    <span style={{ fontFamily: font.mono, fontSize: 13, color: colors.text }}>{term.value}</span>
                    {term.context && (
                      <span style={{ fontSize: 11, color: colors.textMuted, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {term.context}
                      </span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Analyst Notes */}
          {finding.analyst_notes && (
            <div style={sectionStyle}>
              <div style={sectionTitle}><User size={14} /> Analyst Notes</div>
              <div style={{
                fontSize: 13, lineHeight: 1.6, color: colors.textDim,
                whiteSpace: 'pre-wrap', padding: '12px 16px',
                background: colors.bgSurface, borderRadius: 6,
                border: `1px solid ${colors.border}`,
              }}>
                {finding.analyst_notes}
              </div>
            </div>
          )}
        </div>

        {/* Right column - sidebar */}
        <div>
          {/* Tags */}
          {finding.tags && finding.tags.length > 0 && (
            <div style={sectionStyle}>
              <div style={sectionTitle}><Tag size={14} /> Tags</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {finding.tags.map((tag, i) => (
                  <span key={i} style={{
                    fontSize: 11, padding: '3px 10px', borderRadius: 9999,
                    background: colors.bgSurface, border: `1px solid ${colors.border}`,
                    color: colors.textDim, fontFamily: font.mono,
                  }}>
                    {tag}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Enrichment Data */}
          {finding.enrichment && (
            <div style={sectionStyle}>
              <div style={sectionTitle}><Shield size={14} /> Enrichment</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                {finding.enrichment.dedup && (
                  <div>
                    <div style={labelStyle}>Dedup</div>
                    <div style={valueStyle}>
                      Similarity: {(finding.enrichment.dedup.similarity_score ?? 0).toFixed(2)}
                      <span style={{ color: colors.textMuted, marginLeft: 8 }}>Action: {finding.enrichment.dedup.action}</span>
                    </div>
                  </div>
                )}
                {finding.enrichment.false_positive && (
                  <div>
                    <div style={labelStyle}>False Positive Check</div>
                    <div style={valueStyle}>
                      <span style={{ color: finding.enrichment.false_positive.is_fp ? colors.statusDismissed : colors.healthy }}>
                        {finding.enrichment.false_positive.is_fp ? 'Likely FP' : 'Not FP'}
                      </span>
                      <span style={{ color: colors.textMuted, marginLeft: 8 }}>
                        Confidence: {((finding.enrichment.false_positive.confidence ?? 0) * 100).toFixed(0)}%
                      </span>
                    </div>
                    {finding.enrichment.false_positive.reason && (
                      <div style={{ fontSize: 11, color: colors.textMuted, marginTop: 2 }}>{finding.enrichment.false_positive.reason}</div>
                    )}
                  </div>
                )}
                {finding.enrichment.threat_intel && Object.keys(finding.enrichment.threat_intel).length > 0 && (
                  <div>
                    <div style={labelStyle}>Threat Intel</div>
                    <pre style={{
                      fontFamily: font.mono, fontSize: 11, lineHeight: 1.5,
                      color: colors.textDim, whiteSpace: 'pre-wrap', margin: 0,
                    }}>
                      {JSON.stringify(finding.enrichment.threat_intel, null, 2)}
                    </pre>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Status History */}
          {finding.status_history && finding.status_history.length > 0 && (
            <div style={sectionStyle}>
              <div style={sectionTitle}><Clock size={14} /> Status History</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
                {finding.status_history.map((entry, i) => {
                  const color = statusColor(entry.status);
                  return (
                    <div key={i} style={{
                      display: 'flex', gap: 12, padding: '8px 0',
                      borderBottom: i < finding.status_history.length - 1 ? `1px solid ${colors.border}` : 'none',
                    }}>
                      <div style={{
                        width: 8, height: 8, borderRadius: '50%', background: color,
                        marginTop: 5, flexShrink: 0,
                      }} />
                      <div style={{ flex: 1 }}>
                        <div style={{ fontSize: 12, color: colors.text, fontWeight: 500 }}>
                          {entry.status.charAt(0).toUpperCase() + entry.status.slice(1)}
                        </div>
                        <div style={{ fontSize: 11, color: colors.textMuted }}>
                          {new Date(entry.changed_at).toLocaleString()}
                          {entry.changed_by && <> &middot; {entry.changed_by}</>}
                        </div>
                        {entry.notes && (
                          <div style={{ fontSize: 11, color: colors.textDim, marginTop: 2 }}>{entry.notes}</div>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
