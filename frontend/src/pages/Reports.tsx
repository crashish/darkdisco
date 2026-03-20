import { useState, useEffect, useCallback } from 'react';
import { fetchInstitutions, fetchSources, fetchReportTemplates, createReportTemplate, updateReportTemplate, deleteReportTemplate } from '../api';
import { colors, card, font, statusLabel } from '../theme';
import MultiSelect from '../components/MultiSelect';
import type { Institution, Source, Severity, FindingStatus, ReportSections, ReportChartOptions, ReportTemplate } from '../types';
import { Calendar, Download, Loader2, Eye, ChevronDown, ChevronRight, Save, FolderOpen, Trash2, Pencil } from 'lucide-react';
import type { CSSProperties } from 'react';

const allSeverities: Severity[] = ['critical', 'high', 'medium', 'low', 'info'];
const allStatuses: FindingStatus[] = ['new', 'reviewing', 'escalated', 'confirmed', 'dismissed', 'false_positive', 'resolved'];

const defaultSections: ReportSections = {
  executive_summary: true,
  charts: true,
  findings_detail: true,
  findings_by_severity: true,
  source_activity: true,
  institution_exposure: true,
  classification_breakdown: true,
  timeline: true,
};

const defaultCharts: ReportChartOptions = {
  severity_pie: true,
  status_pie: true,
  trend_line: true,
  source_bar: true,
  institution_bar: true,
  severity_trend: true,
};

const sectionLabels: Record<keyof ReportSections, string> = {
  executive_summary: 'Executive Summary',
  charts: 'Charts',
  findings_detail: 'Findings Detail',
  findings_by_severity: 'Findings by Severity',
  source_activity: 'Source Activity',
  institution_exposure: 'Institution Exposure',
  classification_breakdown: 'Classification Breakdown',
  timeline: 'Timeline',
};

const chartLabels: Record<keyof ReportChartOptions, string> = {
  severity_pie: 'Severity Pie',
  status_pie: 'Status Pie',
  trend_line: 'Trend Line',
  source_bar: 'Source Bar',
  institution_bar: 'Institution Bar',
  severity_trend: 'Severity Trend',
};

const selectStyle: CSSProperties = {
  background: colors.bgSurface,
  border: `1px solid ${colors.border}`,
  borderRadius: 6,
  color: colors.text,
  padding: '8px 12px',
  fontSize: 13,
  outline: 'none',
  cursor: 'pointer',
  appearance: 'none' as const,
  minWidth: 140,
};

const inputStyle: CSSProperties = {
  ...selectStyle,
  width: '100%',
};

const btnPrimary: CSSProperties = {
  background: colors.accent,
  color: '#fff',
  border: 'none',
  borderRadius: 6,
  padding: '10px 20px',
  fontSize: 14,
  fontWeight: 600,
  cursor: 'pointer',
  display: 'inline-flex',
  alignItems: 'center',
  gap: 8,
  fontFamily: font.sans,
};

const btnSecondary: CSSProperties = {
  ...btnPrimary,
  background: colors.bgSurface,
  border: `1px solid ${colors.border}`,
  color: colors.text,
};

const checkboxRowStyle: CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  padding: '6px 0',
  fontSize: 13,
  color: colors.text,
  cursor: 'pointer',
};

function parseDateShorthand(input: string): { from: string; to: string } | null {
  const trimmed = input.trim().toLowerCase();
  const match = trimmed.match(/^(\d+)\s*(h|d|w|m)$/);
  if (!match) return null;
  const amount = parseInt(match[1], 10);
  const unit = match[2];
  const now = new Date();
  const from = new Date(now);
  switch (unit) {
    case 'h': from.setHours(from.getHours() - amount); break;
    case 'd': from.setDate(from.getDate() - amount); break;
    case 'w': from.setDate(from.getDate() - amount * 7); break;
    case 'm': from.setMonth(from.getMonth() - amount); break;
    default: return null;
  }
  const fmt = (d: Date) => d.toISOString().slice(0, 10);
  return { from: fmt(from), to: fmt(now) };
}

export default function Reports() {
  const [title, setTitle] = useState('DarkDisco Threat Intelligence Report');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [institutions, setInstitutions] = useState<Institution[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [sevFilter, setSevFilter] = useState<Set<string>>(new Set());
  const [statusFilter, setStatusFilter] = useState<Set<string>>(new Set());
  const [instFilter, setInstFilter] = useState<Set<string>>(new Set());
  const [sourceFilter, setSourceFilter] = useState<Set<string>>(new Set());
  const [sections, setSections] = useState<ReportSections>({ ...defaultSections });
  const [charts, setCharts] = useState<ReportChartOptions>({ ...defaultCharts });
  const [generating, setGenerating] = useState(false);
  const [previewing, setPreviewing] = useState(false);
  const [previewHtml, setPreviewHtml] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [sectionsOpen, setSectionsOpen] = useState(true);
  const [chartsOpen, setChartsOpen] = useState(false);
  const [templates, setTemplates] = useState<ReportTemplate[]>([]);
  const [selectedTemplateId, setSelectedTemplateId] = useState<string>('');
  const [saveTemplateName, setSaveTemplateName] = useState('');
  const [saveTemplateDesc, setSaveTemplateDesc] = useState('');
  const [showSaveDialog, setShowSaveDialog] = useState(false);
  const [editingTemplateId, setEditingTemplateId] = useState<string | null>(null);

  const loadTemplates = useCallback(() => {
    fetchReportTemplates().then(setTemplates).catch(() => {});
  }, []);

  useEffect(() => {
    fetchInstitutions().then(setInstitutions);
    fetchSources().then(setSources);
    loadTemplates();
  }, [loadTemplates]);

  const buildRequest = useCallback(() => {
    const req: Record<string, unknown> = {
      title,
      sections,
      charts,
    };
    if (dateFrom) req.date_from = dateFrom.includes('T') ? dateFrom : `${dateFrom}T00:00:00`;
    if (dateTo) req.date_to = dateTo.includes('T') ? dateTo : `${dateTo}T23:59:59`;
    if (sevFilter.size > 0) req.severities = Array.from(sevFilter);
    if (statusFilter.size > 0) req.statuses = Array.from(statusFilter);
    if (instFilter.size === 1) req.institution_id = Array.from(instFilter)[0];
    return req;
  }, [title, dateFrom, dateTo, sevFilter, statusFilter, instFilter, sections, charts]);

  const loadTemplate = (tmpl: ReportTemplate) => {
    const c = tmpl.config;
    setTitle(c.title || 'DarkDisco Threat Intelligence Report');
    setSevFilter(c.severities ? new Set(c.severities) : new Set());
    setStatusFilter(c.statuses ? new Set(c.statuses) : new Set());
    setInstFilter(c.institution_id ? new Set([c.institution_id]) : new Set());
    setSections(c.sections || { ...defaultSections });
    setCharts(c.charts || { ...defaultCharts });
    setSelectedTemplateId(tmpl.id);
  };

  const handleSaveTemplate = async () => {
    if (!saveTemplateName.trim()) return;
    const config = {
      title,
      client_id: undefined as string | undefined,
      institution_id: instFilter.size === 1 ? Array.from(instFilter)[0] : undefined,
      severities: sevFilter.size > 0 ? Array.from(sevFilter) : undefined,
      statuses: statusFilter.size > 0 ? Array.from(statusFilter) : undefined,
      sections,
      charts,
    };
    if (editingTemplateId) {
      await updateReportTemplate(editingTemplateId, { name: saveTemplateName, description: saveTemplateDesc || null, config });
    } else {
      await createReportTemplate(saveTemplateName, saveTemplateDesc || null, config);
    }
    setShowSaveDialog(false);
    setSaveTemplateName('');
    setSaveTemplateDesc('');
    setEditingTemplateId(null);
    loadTemplates();
  };

  const handleDeleteTemplate = async (id: string) => {
    await deleteReportTemplate(id);
    if (selectedTemplateId === id) setSelectedTemplateId('');
    loadTemplates();
  };

  const handleEditTemplate = (tmpl: ReportTemplate) => {
    setSaveTemplateName(tmpl.name);
    setSaveTemplateDesc(tmpl.description || '');
    setEditingTemplateId(tmpl.id);
    setShowSaveDialog(true);
  };

  const handleGenerate = async () => {
    setGenerating(true);
    setError(null);
    try {
      const token = localStorage.getItem('dd_token');
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;
      const res = await fetch('/api/reports/generate', {
        method: 'POST',
        headers,
        body: JSON.stringify(buildRequest()),
      });
      if (res.status === 401) {
        localStorage.removeItem('dd_token');
        window.dispatchEvent(new CustomEvent('auth:logout', { detail: 'unauthorized' }));
        return;
      }
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `Error ${res.status}`);
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      const disp = res.headers.get('content-disposition');
      const match = disp?.match(/filename="?([^"]+)"?/);
      a.download = match?.[1] || 'report.pdf';
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to generate report');
    } finally {
      setGenerating(false);
    }
  };

  const handlePreview = async () => {
    setPreviewing(true);
    setError(null);
    setPreviewHtml(null);
    try {
      const token = localStorage.getItem('dd_token');
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;
      const res = await fetch('/api/reports/preview', {
        method: 'POST',
        headers,
        body: JSON.stringify(buildRequest()),
      });
      if (res.status === 401) {
        localStorage.removeItem('dd_token');
        window.dispatchEvent(new CustomEvent('auth:logout', { detail: 'unauthorized' }));
        return;
      }
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `Error ${res.status}`);
      }
      const data = await res.json();
      setPreviewHtml(data.html);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to generate preview');
    } finally {
      setPreviewing(false);
    }
  };

  const toggleSection = (key: keyof ReportSections) => {
    setSections(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const toggleChart = (key: keyof ReportChartOptions) => {
    setCharts(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const allSectionsSelected = Object.values(sections).every(Boolean);
  const allChartsSelected = Object.values(charts).every(Boolean);

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 700, marginBottom: 4 }}>Reports</h1>
      <p style={{ color: colors.textDim, fontSize: 14, marginBottom: 24 }}>
        Generate threat intelligence reports as PDF
      </p>

      {/* Templates */}
      <div style={{ ...card, marginBottom: 24 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: templates.length > 0 ? 12 : 0 }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: colors.textDim, textTransform: 'uppercase', letterSpacing: '0.05em', display: 'flex', alignItems: 'center', gap: 6 }}>
            <FolderOpen size={14} />
            Saved Templates
          </label>
          <button
            onClick={() => { setEditingTemplateId(null); setSaveTemplateName(''); setSaveTemplateDesc(''); setShowSaveDialog(true); }}
            style={{ ...btnSecondary, padding: '6px 12px', fontSize: 12 }}
          >
            <Save size={14} />
            Save Current
          </button>
        </div>
        {templates.length > 0 && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
            {templates.map(tmpl => (
              <div
                key={tmpl.id}
                style={{
                  display: 'flex', alignItems: 'center', gap: 6,
                  padding: '6px 10px', borderRadius: 6,
                  border: `1px solid ${selectedTemplateId === tmpl.id ? colors.accent : colors.border}`,
                  background: selectedTemplateId === tmpl.id ? colors.accent + '15' : colors.bgSurface,
                  fontSize: 13,
                }}
              >
                <button
                  onClick={() => loadTemplate(tmpl)}
                  style={{ background: 'none', border: 'none', color: colors.text, cursor: 'pointer', padding: 0, fontSize: 13 }}
                  title={tmpl.description || 'Load template'}
                >
                  {tmpl.name}
                </button>
                <button
                  onClick={() => handleEditTemplate(tmpl)}
                  style={{ background: 'none', border: 'none', color: colors.textMuted, cursor: 'pointer', padding: 0 }}
                  title="Edit template"
                >
                  <Pencil size={12} />
                </button>
                <button
                  onClick={() => handleDeleteTemplate(tmpl.id)}
                  style={{ background: 'none', border: 'none', color: colors.textMuted, cursor: 'pointer', padding: 0 }}
                  title="Delete template"
                >
                  <Trash2 size={12} />
                </button>
              </div>
            ))}
          </div>
        )}
        {templates.length === 0 && (
          <p style={{ color: colors.textMuted, fontSize: 12, marginTop: 8 }}>
            No saved templates. Configure your report and click "Save Current" to create one.
          </p>
        )}
      </div>

      {/* Save Template Dialog */}
      {showSaveDialog && (
        <div style={{
          ...card, marginBottom: 24,
          border: `1px solid ${colors.accent}`,
        }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: colors.textDim, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 10, display: 'block' }}>
            {editingTemplateId ? 'Edit Template' : 'Save as Template'}
          </label>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            <input
              type="text"
              value={saveTemplateName}
              onChange={e => setSaveTemplateName(e.target.value)}
              placeholder="Template name..."
              style={inputStyle}
              autoFocus
            />
            <input
              type="text"
              value={saveTemplateDesc}
              onChange={e => setSaveTemplateDesc(e.target.value)}
              placeholder="Description (optional)..."
              style={inputStyle}
            />
            <div style={{ display: 'flex', gap: 8 }}>
              <button
                onClick={handleSaveTemplate}
                disabled={!saveTemplateName.trim()}
                style={{ ...btnPrimary, padding: '8px 16px', fontSize: 13, opacity: saveTemplateName.trim() ? 1 : 0.5 }}
              >
                {editingTemplateId ? 'Update' : 'Save'}
              </button>
              <button
                onClick={() => { setShowSaveDialog(false); setEditingTemplateId(null); }}
                style={{ ...btnSecondary, padding: '8px 16px', fontSize: 13 }}
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
        {/* Left column: Configuration */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
          {/* Report Title */}
          <div style={{ ...card }}>
            <label style={{ fontSize: 12, fontWeight: 600, color: colors.textDim, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 8, display: 'block' }}>
              Report Title
            </label>
            <input
              type="text"
              value={title}
              onChange={e => setTitle(e.target.value)}
              style={inputStyle}
              placeholder="Report title..."
            />
          </div>

          {/* Date Range */}
          <div style={{ ...card }}>
            <label style={{ fontSize: 12, fontWeight: 600, color: colors.textDim, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 12, display: 'block' }}>
              Date Range
            </label>
            <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 10 }}>
              <Calendar size={16} color={colors.textMuted} />
              <input
                type="date"
                style={{ ...selectStyle, minWidth: 140, padding: '7px 10px' }}
                value={dateFrom}
                onChange={e => setDateFrom(e.target.value)}
                title="From date"
              />
              <span style={{ color: colors.textMuted, fontSize: 12 }}>&ndash;</span>
              <input
                type="date"
                style={{ ...selectStyle, minWidth: 140, padding: '7px 10px' }}
                value={dateTo}
                onChange={e => setDateTo(e.target.value)}
                title="To date"
              />
              <input
                type="text"
                placeholder="e.g. 30d, 1w, 3m"
                style={{ ...selectStyle, minWidth: 110, padding: '7px 10px', fontSize: 12 }}
                onKeyDown={e => {
                  if (e.key === 'Enter') {
                    const range = parseDateShorthand((e.target as HTMLInputElement).value);
                    if (range) { setDateFrom(range.from); setDateTo(range.to); (e.target as HTMLInputElement).value = ''; }
                  }
                }}
                title="Type shorthand (24h, 7d, 1w, 3m) and press Enter"
              />
            </div>
            <div style={{ display: 'flex', gap: 4 }}>
              {[
                { label: '24h', value: '24h' },
                { label: '7d', value: '7d' },
                { label: '30d', value: '30d' },
                { label: '90d', value: '90d' },
              ].map(btn => (
                <button
                  key={btn.value}
                  onClick={() => { const range = parseDateShorthand(btn.value); if (range) { setDateFrom(range.from); setDateTo(range.to); } }}
                  style={{
                    background: 'none', border: `1px solid ${colors.border}`, borderRadius: 4,
                    color: colors.accent, fontSize: 11, padding: '4px 8px', cursor: 'pointer',
                  }}
                >
                  {btn.label}
                </button>
              ))}
              {(dateFrom || dateTo) && (
                <button
                  onClick={() => { setDateFrom(''); setDateTo(''); }}
                  style={{
                    background: 'none', border: 'none',
                    color: colors.accent, fontSize: 11, padding: '4px 8px', cursor: 'pointer',
                  }}
                >
                  Clear
                </button>
              )}
            </div>
          </div>

          {/* Filters */}
          <div style={{ ...card }}>
            <label style={{ fontSize: 12, fontWeight: 600, color: colors.textDim, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 12, display: 'block' }}>
              Filters
            </label>
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
              <MultiSelect
                label="Severity"
                options={allSeverities.map(s => ({ value: s, label: s.charAt(0).toUpperCase() + s.slice(1) }))}
                selected={sevFilter}
                onChange={setSevFilter}
              />
              <MultiSelect
                label="Status"
                options={allStatuses.map(s => ({ value: s, label: statusLabel(s) }))}
                selected={statusFilter}
                onChange={setStatusFilter}
              />
              <MultiSelect
                label="Institution"
                options={institutions.map(i => ({ value: i.id, label: i.name }))}
                selected={instFilter}
                onChange={setInstFilter}
              />
              <MultiSelect
                label="Source"
                options={sources.map(s => ({ value: s.id, label: s.name }))}
                selected={sourceFilter}
                onChange={setSourceFilter}
              />
            </div>
            {(sevFilter.size > 0 || statusFilter.size > 0 || instFilter.size > 0 || sourceFilter.size > 0) && (
              <button
                onClick={() => { setSevFilter(new Set()); setStatusFilter(new Set()); setInstFilter(new Set()); setSourceFilter(new Set()); }}
                style={{ background: 'none', border: 'none', color: colors.accent, fontSize: 12, cursor: 'pointer', padding: '8px 0 0' }}
              >
                Clear all filters
              </button>
            )}
          </div>
        </div>

        {/* Right column: Sections & Charts */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
          {/* Content Sections */}
          <div style={{ ...card }}>
            <button
              onClick={() => setSectionsOpen(o => !o)}
              style={{
                display: 'flex', alignItems: 'center', gap: 8, width: '100%',
                background: 'none', border: 'none', cursor: 'pointer', padding: 0,
                fontSize: 12, fontWeight: 600, color: colors.textDim, textTransform: 'uppercase', letterSpacing: '0.05em',
              }}
            >
              {sectionsOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
              Content Sections
            </button>
            {sectionsOpen && (
              <div style={{ marginTop: 12 }}>
                <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
                  <button
                    onClick={() => setSections({ ...sections, ...Object.fromEntries(Object.keys(sections).map(k => [k, true])) })}
                    disabled={allSectionsSelected}
                    style={{
                      padding: '2px 6px', fontSize: 11, color: colors.accent,
                      background: 'none', border: `1px solid ${colors.border}`, borderRadius: 3,
                      cursor: 'pointer', opacity: allSectionsSelected ? 0.4 : 1,
                    }}
                  >
                    Select All
                  </button>
                  <button
                    onClick={() => setSections({ ...sections, ...Object.fromEntries(Object.keys(sections).map(k => [k, false])) })}
                    disabled={Object.values(sections).every(v => !v)}
                    style={{
                      padding: '2px 6px', fontSize: 11, color: colors.accent,
                      background: 'none', border: `1px solid ${colors.border}`, borderRadius: 3,
                      cursor: 'pointer', opacity: Object.values(sections).every(v => !v) ? 0.4 : 1,
                    }}
                  >
                    Deselect All
                  </button>
                </div>
                {(Object.keys(sectionLabels) as (keyof ReportSections)[]).map(key => (
                  <label key={key} style={checkboxRowStyle}>
                    <input
                      type="checkbox"
                      checked={sections[key]}
                      onChange={() => toggleSection(key)}
                      style={{ accentColor: colors.accent }}
                    />
                    {sectionLabels[key]}
                  </label>
                ))}
              </div>
            )}
          </div>

          {/* Chart Options */}
          <div style={{ ...card }}>
            <button
              onClick={() => setChartsOpen(o => !o)}
              style={{
                display: 'flex', alignItems: 'center', gap: 8, width: '100%',
                background: 'none', border: 'none', cursor: 'pointer', padding: 0,
                fontSize: 12, fontWeight: 600, color: colors.textDim, textTransform: 'uppercase', letterSpacing: '0.05em',
              }}
            >
              {chartsOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
              Chart Options
            </button>
            {chartsOpen && (
              <div style={{ marginTop: 12 }}>
                <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
                  <button
                    onClick={() => setCharts({ ...charts, ...Object.fromEntries(Object.keys(charts).map(k => [k, true])) })}
                    disabled={allChartsSelected}
                    style={{
                      padding: '2px 6px', fontSize: 11, color: colors.accent,
                      background: 'none', border: `1px solid ${colors.border}`, borderRadius: 3,
                      cursor: 'pointer', opacity: allChartsSelected ? 0.4 : 1,
                    }}
                  >
                    Select All
                  </button>
                  <button
                    onClick={() => setCharts({ ...charts, ...Object.fromEntries(Object.keys(charts).map(k => [k, false])) })}
                    disabled={Object.values(charts).every(v => !v)}
                    style={{
                      padding: '2px 6px', fontSize: 11, color: colors.accent,
                      background: 'none', border: `1px solid ${colors.border}`, borderRadius: 3,
                      cursor: 'pointer', opacity: Object.values(charts).every(v => !v) ? 0.4 : 1,
                    }}
                  >
                    Deselect All
                  </button>
                </div>
                {(Object.keys(chartLabels) as (keyof ReportChartOptions)[]).map(key => (
                  <label key={key} style={checkboxRowStyle}>
                    <input
                      type="checkbox"
                      checked={charts[key]}
                      onChange={() => toggleChart(key)}
                      style={{ accentColor: colors.accent }}
                    />
                    {chartLabels[key]}
                  </label>
                ))}
              </div>
            )}
          </div>

          {/* Actions */}
          <div style={{ display: 'flex', gap: 12 }}>
            <button
              onClick={handleGenerate}
              disabled={generating}
              style={{ ...btnPrimary, opacity: generating ? 0.6 : 1 }}
            >
              {generating ? <Loader2 size={16} style={{ animation: 'spin 1s linear infinite' }} /> : <Download size={16} />}
              {generating ? 'Generating...' : 'Generate PDF'}
            </button>
            <button
              onClick={handlePreview}
              disabled={previewing}
              style={{ ...btnSecondary, opacity: previewing ? 0.6 : 1 }}
            >
              {previewing ? <Loader2 size={16} style={{ animation: 'spin 1s linear infinite' }} /> : <Eye size={16} />}
              {previewing ? 'Loading...' : 'Preview'}
            </button>
          </div>

          {error && (
            <div style={{
              ...card,
              borderColor: colors.critical,
              background: colors.criticalBg,
              color: colors.critical,
              fontSize: 13,
            }}>
              {error}
            </div>
          )}
        </div>
      </div>

      {/* HTML Preview */}
      {previewHtml && (
        <div style={{ marginTop: 24 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <h2 style={{ fontSize: 18, fontWeight: 600 }}>Preview</h2>
            <button
              onClick={() => setPreviewHtml(null)}
              style={{ background: 'none', border: 'none', color: colors.accent, fontSize: 13, cursor: 'pointer' }}
            >
              Close preview
            </button>
          </div>
          <div style={{
            ...card,
            padding: 0,
            overflow: 'hidden',
          }}>
            <iframe
              srcDoc={previewHtml}
              style={{
                width: '100%',
                minHeight: 800,
                border: 'none',
                background: '#fff',
              }}
              title="Report Preview"
            />
          </div>
        </div>
      )}

      {/* Spinner keyframes */}
      <style>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
}
