// @ts-nocheck
import { useEffect, useState, useRef } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { fetchFinding, updateFindingStatus, updateFinding, addFindingNote, fetchAuditLog, fetchClassifications, fetchArchiveContents } from '../api';
import { colors, card, font, statusColor } from '../theme';
import SeverityBadge from '../components/SeverityBadge';
import StatusBadge from '../components/StatusBadge';
import ArchiveContents from '../components/ArchiveContents';
import type { ArchiveFile } from '../components/ArchiveContents';
import type { FindingDetail as FindingDetailType, FindingStatus, Severity, AuditLogEntry, HighlightSpan } from '../types';
import { ArrowLeft, ExternalLink, Tag, Clock, User, FileText, Shield, Search, ChevronDown, MessageSquare, Forward, Paperclip, Reply, Hash, Globe, Lock, Activity, BarChart3, Camera, Network, Edit3, Send, History } from 'lucide-react';
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
  const [severityMenuOpen, setSeverityMenuOpen] = useState(false);
  const [archiveFiles, setArchiveFiles] = useState<ArchiveFile[]>([]);
  const [auditLog, setAuditLog] = useState<AuditLogEntry[]>([]);
  const [classifications, setClassifications] = useState<string[]>([]);
  const [classificationInput, setClassificationInput] = useState('');
  const [classificationOpen, setClassificationOpen] = useState(false);
  const [noteInput, setNoteInput] = useState('');
  const [noteSaving, setNoteSaving] = useState(false);
  const classificationRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    fetchFinding(id).then(f => {
      setFinding(f);
      setClassificationInput(f.classification || '');
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
    fetchAuditLog(id).then(setAuditLog).catch(() => {});
    fetchClassifications().then(setClassifications).catch(() => {});
  }, [id]);

  const handleStatusChange = async (status: FindingStatus) => {
    if (!finding) return;
    await updateFindingStatus(finding.id, status);
    setFinding(prev => prev ? { ...prev, status } : prev);
    setStatusMenuOpen(false);
    if (id) fetchAuditLog(id).then(setAuditLog).catch(() => {});
  };

  const handleSeverityChange = async (severity: Severity) => {
    if (!finding) return;
    await updateFinding(finding.id, { severity });
    setFinding(prev => prev ? { ...prev, severity } : prev);
    setSeverityMenuOpen(false);
    if (id) fetchAuditLog(id).then(setAuditLog).catch(() => {});
  };

  const handleClassificationSave = async (value: string) => {
    if (!finding) return;
    await updateFinding(finding.id, { classification: value });
    setFinding(prev => prev ? { ...prev, classification: value } : prev);
    setClassificationOpen(false);
    if (id) fetchAuditLog(id).then(setAuditLog).catch(() => {});
    if (value && !classifications.includes(value)) {
      setClassifications(prev => [...prev, value].sort());
    }
  };

  const handleAddNote = async () => {
    if (!finding || !noteInput.trim()) return;
    setNoteSaving(true);
    try {
      const updated = await addFindingNote(finding.id, noteInput.trim());
      setFinding(prev => prev ? { ...prev, analyst_notes: updated.analyst_notes } : prev);
      setNoteInput('');
      if (id) fetchAuditLog(id).then(setAuditLog).catch(() => {});
    } finally {
      setNoteSaving(false);
    }
  };

  const allSeverities: Severity[] = ['critical', 'high', 'medium', 'low', 'info'];
  const filteredClassifications = classifications.filter(c =>
    c.toLowerCase().includes(classificationInput.toLowerCase())
  );

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
              <div style={{ position: 'relative' }}>
                <div
                  style={{ display: 'inline-flex', alignItems: 'center', gap: 4, cursor: 'pointer' }}
                  onClick={() => setSeverityMenuOpen(!severityMenuOpen)}
                >
                  <SeverityBadge severity={finding.severity} />
                  <Edit3 size={11} color={colors.textMuted} />
                </div>
                {severityMenuOpen && (
                  <div style={{
                    position: 'absolute', top: '100%', left: 0, zIndex: 50, marginTop: 4,
                    background: colors.bgSurface, border: `1px solid ${colors.border}`, borderRadius: 6,
                    padding: 4, minWidth: 120, boxShadow: '0 8px 24px rgba(0,0,0,0.4)',
                  }}>
                    {allSeverities.filter(s => s !== finding.severity).map(s => (
                      <div
                        key={s}
                        style={{ padding: '6px 10px', fontSize: 12, cursor: 'pointer', borderRadius: 4, color: colors.textDim }}
                        onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = colors.bgHover; (e.currentTarget as HTMLElement).style.color = colors.text; }}
                        onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = 'transparent'; (e.currentTarget as HTMLElement).style.color = colors.textDim; }}
                        onClick={() => handleSeverityChange(s)}
                      >
                        {s.charAt(0).toUpperCase() + s.slice(1)}
                      </div>
                    ))}
                  </div>
                )}
              </div>
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

          {/* Trapline Intelligence */}
          {finding.metadata && (finding.metadata as Record<string, unknown>).trapline && (() => {
            const trap = (finding.metadata as Record<string, Record<string, unknown>>).trapline;
            const dns = trap.dns_records as Record<string, unknown> | undefined;
            const whois = trap.whois as Record<string, unknown> | undefined;
            const tls = trap.tls_certificate as Record<string, unknown> | undefined;
            const screenshotUrl = trap.screenshot_url as string | undefined;
            const networkLog = trap.network_log as Array<Record<string, string>> | undefined;
            const scoreBreakdown = trap.score_breakdown as Array<Record<string, unknown>> | undefined;
            const trapScore = trap.score as number | undefined;

            const subSection: CSSProperties = {
              padding: '12px 16px',
              borderBottom: `1px solid ${colors.border}`,
            };
            const subTitle: CSSProperties = {
              fontSize: 11, fontWeight: 600, color: colors.textMuted,
              textTransform: 'uppercase', letterSpacing: '0.05em',
              marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6,
            };
            const kvRow: CSSProperties = {
              display: 'flex', gap: 8, fontSize: 12, marginBottom: 4,
            };
            const kvLabel: CSSProperties = {
              color: colors.textMuted, minWidth: 100, flexShrink: 0,
            };
            const kvValue: CSSProperties = {
              color: colors.text, fontFamily: font.mono, wordBreak: 'break-all',
            };

            return (
              <div style={sectionStyle}>
                <div style={sectionTitle}><Globe size={14} /> Trapline Intelligence</div>
                <div style={{
                  background: colors.bgSurface, borderRadius: 6,
                  border: `1px solid ${colors.border}`, overflow: 'hidden',
                }}>
                  {/* Score header */}
                  {trapScore !== undefined && (
                    <div style={{
                      ...subSection, display: 'flex', alignItems: 'center', gap: 12,
                    }}>
                      <div style={{
                        fontSize: 28, fontWeight: 700, fontFamily: font.mono,
                        color: trapScore >= 70 ? colors.statusEscalated : trapScore >= 40 ? '#f59e0b' : colors.textMuted,
                      }}>
                        {trapScore}
                      </div>
                      <div>
                        <div style={{ fontSize: 12, color: colors.textDim }}>Trapline Score</div>
                        <div style={{ fontSize: 11, color: colors.textMuted }}>
                          {trapScore >= 70 ? 'High risk' : trapScore >= 40 ? 'Medium risk' : 'Low risk'}
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Score Breakdown */}
                  {scoreBreakdown && scoreBreakdown.length > 0 && (
                    <div style={subSection}>
                      <div style={subTitle}><BarChart3 size={12} /> Score Breakdown</div>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                        {scoreBreakdown.map((s, i) => (
                          <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 12 }}>
                            <span style={{
                              fontFamily: font.mono, fontWeight: 600,
                              color: colors.accent, minWidth: 30, textAlign: 'right',
                            }}>
                              +{String(s.weight)}
                            </span>
                            <span style={{ color: colors.text }}>{String(s.signal)}</span>
                            {s.detail && (
                              <span style={{ color: colors.textMuted, fontSize: 11 }}>{String(s.detail)}</span>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Screenshot */}
                  {screenshotUrl && (
                    <div style={subSection}>
                      <div style={subTitle}><Camera size={12} /> Screenshot</div>
                      <a href={screenshotUrl} target="_blank" rel="noopener noreferrer">
                        <img
                          src={screenshotUrl}
                          alt="Phishing site screenshot"
                          style={{
                            maxWidth: '100%', borderRadius: 4,
                            border: `1px solid ${colors.border}`,
                          }}
                          onError={(e) => {
                            (e.currentTarget as HTMLImageElement).style.display = 'none';
                            const fallback = e.currentTarget.nextElementSibling as HTMLElement | null;
                            if (fallback) fallback.style.display = 'flex';
                          }}
                        />
                      </a>
                      <a
                        href={screenshotUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{ display: 'none', alignItems: 'center', gap: 4, color: colors.accent, fontSize: 12, marginTop: 4 }}
                      >
                        <ExternalLink size={12} /> View screenshot
                      </a>
                    </div>
                  )}

                  {/* DNS Records */}
                  {dns && (
                    <div style={subSection}>
                      <div style={subTitle}><Network size={12} /> DNS Records</div>
                      {(['A', 'CNAME', 'MX', 'NS'] as const).map(rtype => {
                        const records = dns[rtype] as string[] | undefined;
                        if (!records || records.length === 0) return null;
                        return (
                          <div key={rtype} style={kvRow}>
                            <span style={kvLabel}>{rtype}</span>
                            <span style={kvValue}>{records.join(', ')}</span>
                          </div>
                        );
                      })}
                      {(dns.resolved_ips as Array<Record<string, string>> | undefined)?.map((ip, i) => (
                        <div key={i} style={kvRow}>
                          <span style={kvLabel}>{i === 0 ? 'Resolved IPs' : ''}</span>
                          <span style={kvValue}>
                            {ip.ip}
                            {ip.asn && <span style={{ color: colors.textMuted }}> (AS{ip.asn}{ip.org ? ` - ${ip.org}` : ''})</span>}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* WHOIS */}
                  {whois && (
                    <div style={subSection}>
                      <div style={subTitle}><FileText size={12} /> WHOIS</div>
                      {[
                        ['Registrar', whois.registrar],
                        ['Created', whois.creation_date],
                        ['Expires', whois.expiry_date],
                        ['Registrant', whois.registrant_org],
                        ['Country', whois.registrant_country],
                        ['Name Servers', Array.isArray(whois.name_servers) ? (whois.name_servers as string[]).join(', ') : whois.name_servers],
                      ].filter(([, v]) => v).map(([label, value]) => (
                        <div key={String(label)} style={kvRow}>
                          <span style={kvLabel}>{String(label)}</span>
                          <span style={kvValue}>{String(value)}</span>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* TLS Certificate */}
                  {tls && (
                    <div style={subSection}>
                      <div style={subTitle}><Lock size={12} /> TLS Certificate</div>
                      {[
                        ['Issuer', tls.issuer],
                        ['Subject', tls.subject],
                        ['Valid From', tls.not_before],
                        ['Valid Until', tls.not_after],
                        ['Serial', tls.serial_number],
                      ].filter(([, v]) => v).map(([label, value]) => (
                        <div key={String(label)} style={kvRow}>
                          <span style={kvLabel}>{String(label)}</span>
                          <span style={kvValue}>{String(value)}</span>
                        </div>
                      ))}
                      {Array.isArray(tls.sans) && (tls.sans as string[]).length > 0 && (
                        <div style={kvRow}>
                          <span style={kvLabel}>SANs</span>
                          <span style={kvValue}>{(tls.sans as string[]).join(', ')}</span>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Network Log */}
                  {networkLog && networkLog.length > 0 && (
                    <div style={{ ...subSection, borderBottom: 'none' }}>
                      <div style={subTitle}><Activity size={12} /> Network Log ({networkLog.length} requests)</div>
                      <div style={{ maxHeight: 200, overflow: 'auto' }}>
                        <table style={{ width: '100%', fontSize: 11, borderCollapse: 'collapse' }}>
                          <thead>
                            <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                              <th style={{ textAlign: 'left', padding: '4px 8px', color: colors.textMuted, fontWeight: 500 }}>Domain</th>
                              <th style={{ textAlign: 'left', padding: '4px 8px', color: colors.textMuted, fontWeight: 500 }}>Type</th>
                              <th style={{ textAlign: 'left', padding: '4px 8px', color: colors.textMuted, fontWeight: 500 }}>Status</th>
                            </tr>
                          </thead>
                          <tbody>
                            {networkLog.map((entry, i) => (
                              <tr key={i} style={{ borderBottom: `1px solid ${colors.border}` }}>
                                <td style={{ padding: '4px 8px', fontFamily: font.mono, color: colors.text }}>{entry.domain}</td>
                                <td style={{ padding: '4px 8px', color: colors.textDim }}>{entry.resource_type}</td>
                                <td style={{ padding: '4px 8px', color: colors.textDim }}>{entry.status}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
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

          {/* Classification */}
          <div style={sectionStyle}>
            <div style={sectionTitle}><Tag size={14} /> Classification</div>
            <div style={{ position: 'relative' }} ref={classificationRef}>
              <input
                type="text"
                value={classificationInput}
                onChange={e => { setClassificationInput(e.target.value); setClassificationOpen(true); }}
                onFocus={() => setClassificationOpen(true)}
                onKeyDown={e => {
                  if (e.key === 'Enter') { handleClassificationSave(classificationInput.trim()); }
                  if (e.key === 'Escape') { setClassificationOpen(false); }
                }}
                placeholder="e.g. credential theft, phishing kit, data leak"
                style={{
                  width: '100%', boxSizing: 'border-box',
                  padding: '8px 12px', fontSize: 13,
                  background: colors.bgSurface, color: colors.text,
                  border: `1px solid ${colors.border}`, borderRadius: 6,
                  outline: 'none', fontFamily: font.mono,
                }}
              />
              {classificationOpen && filteredClassifications.length > 0 && (
                <div style={{
                  position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 50, marginTop: 2,
                  background: colors.bgSurface, border: `1px solid ${colors.border}`, borderRadius: 6,
                  maxHeight: 160, overflow: 'auto', boxShadow: '0 8px 24px rgba(0,0,0,0.4)',
                }}>
                  {filteredClassifications.map(c => (
                    <div
                      key={c}
                      style={{ padding: '6px 12px', fontSize: 12, cursor: 'pointer', color: colors.textDim }}
                      onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = colors.bgHover; }}
                      onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = 'transparent'; }}
                      onMouseDown={e => { e.preventDefault(); setClassificationInput(c); handleClassificationSave(c); }}
                    >
                      {c}
                    </div>
                  ))}
                </div>
              )}
              {finding.classification && classificationInput !== finding.classification && (
                <div style={{ fontSize: 11, color: colors.textMuted, marginTop: 4 }}>
                  Current: <span style={{ color: colors.text }}>{finding.classification}</span>
                </div>
              )}
            </div>
          </div>

          {/* Analyst Notes (threaded) */}
          <div style={sectionStyle}>
            <div style={sectionTitle}><User size={14} /> Analyst Notes</div>
            {finding.analyst_notes && (
              <div style={{
                marginBottom: 12, display: 'flex', flexDirection: 'column', gap: 0,
              }}>
                {finding.analyst_notes.split('\n---\n').map((note, i) => (
                  <div key={i} style={{
                    padding: '10px 14px',
                    borderBottom: `1px solid ${colors.border}`,
                    fontSize: 13, lineHeight: 1.5, color: colors.textDim,
                    whiteSpace: 'pre-wrap',
                    background: i % 2 === 0 ? colors.bgSurface : 'transparent',
                  }}>
                    {note}
                  </div>
                ))}
              </div>
            )}
            <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start' }}>
              <textarea
                value={noteInput}
                onChange={e => setNoteInput(e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) handleAddNote(); }}
                placeholder="Add a note... (Ctrl+Enter to submit)"
                rows={2}
                style={{
                  flex: 1, padding: '8px 12px', fontSize: 13,
                  background: colors.bgSurface, color: colors.text,
                  border: `1px solid ${colors.border}`, borderRadius: 6,
                  outline: 'none', resize: 'vertical', fontFamily: 'inherit',
                  lineHeight: 1.5,
                }}
              />
              <button
                onClick={handleAddNote}
                disabled={noteSaving || !noteInput.trim()}
                style={{
                  padding: '8px 12px', background: colors.accent, color: '#fff',
                  border: 'none', borderRadius: 6, cursor: 'pointer',
                  opacity: noteSaving || !noteInput.trim() ? 0.5 : 1,
                  display: 'flex', alignItems: 'center', gap: 4, fontSize: 12,
                }}
              >
                <Send size={12} /> Add
              </button>
            </div>
          </div>
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

          {/* Audit Log */}
          {auditLog.length > 0 && (
            <div style={sectionStyle}>
              <div style={sectionTitle}><History size={14} /> Audit Log</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 0, maxHeight: 400, overflow: 'auto' }}>
                {auditLog.map((entry, i) => {
                  const actionLabels: Record<string, string> = {
                    status_change: 'Status changed',
                    severity_change: 'Severity changed',
                    classification_change: 'Classification changed',
                    note_added: 'Note added',
                  };
                  return (
                    <div key={entry.id} style={{
                      padding: '8px 0',
                      borderBottom: i < auditLog.length - 1 ? `1px solid ${colors.border}` : 'none',
                    }}>
                      <div style={{ fontSize: 12, color: colors.text, fontWeight: 500 }}>
                        {actionLabels[entry.action] || entry.action}
                      </div>
                      {entry.old_value && entry.new_value && (
                        <div style={{ fontSize: 11, color: colors.textDim, marginTop: 2 }}>
                          <span style={{ textDecoration: 'line-through', color: colors.textMuted }}>{entry.old_value}</span>
                          {' → '}
                          <span style={{ color: colors.accent }}>{entry.new_value}</span>
                        </div>
                      )}
                      {!entry.old_value && entry.new_value && entry.action === 'note_added' && (
                        <div style={{ fontSize: 11, color: colors.textDim, marginTop: 2, fontStyle: 'italic' }}>
                          {entry.new_value.length > 80 ? entry.new_value.slice(0, 80) + '...' : entry.new_value}
                        </div>
                      )}
                      {!entry.old_value && entry.new_value && entry.action !== 'note_added' && (
                        <div style={{ fontSize: 11, color: colors.textDim, marginTop: 2 }}>
                          Set to: <span style={{ color: colors.accent }}>{entry.new_value}</span>
                        </div>
                      )}
                      <div style={{ fontSize: 10, color: colors.textMuted, marginTop: 2 }}>
                        {new Date(entry.created_at).toLocaleString()}
                        {entry.user && <> &middot; {entry.user}</>}
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
