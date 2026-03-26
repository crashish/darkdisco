import { useState, useEffect, useCallback } from 'react';
import { fetchMatchingFilters, updateMatchingFilters, testMatchingFilters } from '../api';
import type { MatchingFilters, MatchingFiltersTestResult } from '../api';
import { colors, card, font } from '../theme';
import { Settings, Shield, ShieldOff, FlaskConical, Plus, Trash2, Save, AlertTriangle, CheckCircle2, XCircle, ChevronDown, ChevronUp } from 'lucide-react';
import type { CSSProperties } from 'react';

const sectionStyle: CSSProperties = {
  ...card,
  marginBottom: 16,
};

const sectionTitle: CSSProperties = {
  fontSize: 13,
  fontWeight: 600,
  color: colors.textDim,
  textTransform: 'uppercase' as const,
  letterSpacing: '0.05em',
  marginBottom: 12,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
};

const inputStyle: CSSProperties = {
  background: colors.bgSurface,
  border: `1px solid ${colors.border}`,
  borderRadius: 6,
  padding: '8px 12px',
  color: colors.text,
  fontSize: 13,
  fontFamily: font.mono,
  outline: 'none',
  width: '100%',
};

const btnStyle: CSSProperties = {
  background: colors.accent,
  color: '#fff',
  border: 'none',
  borderRadius: 6,
  padding: '10px 20px',
  fontSize: 14,
  fontWeight: 600,
  cursor: 'pointer',
  display: 'flex',
  alignItems: 'center',
  gap: 8,
};

const btnDanger: CSSProperties = {
  ...btnStyle,
  background: 'transparent',
  color: colors.critical,
  padding: '6px 8px',
};

const btnSmall: CSSProperties = {
  ...btnStyle,
  padding: '6px 12px',
  fontSize: 12,
};

const tagStyle: CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: 6,
  padding: '4px 10px',
  borderRadius: 6,
  fontSize: 12,
  fontFamily: font.mono,
  background: colors.bgSurface,
  border: `1px solid ${colors.border}`,
  color: colors.text,
};

function PatternList({
  title,
  icon,
  items,
  onAdd,
  onRemove,
  onEdit,
  placeholder,
  isMono,
  validationErrors,
}: {
  title: string;
  icon: React.ReactNode;
  items: string[];
  onAdd: () => void;
  onRemove: (idx: number) => void;
  onEdit: (idx: number, value: string) => void;
  placeholder: string;
  isMono?: boolean;
  validationErrors?: Map<number, string>;
}) {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <div style={sectionStyle}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: collapsed ? 0 : 12 }}>
        <div style={sectionTitle}>
          {icon}
          {title}
          <span style={{ fontSize: 12, color: colors.textMuted, fontWeight: 400, textTransform: 'none' }}>
            ({items.length})
          </span>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <button
            onClick={() => setCollapsed(!collapsed)}
            style={{ background: 'transparent', border: 'none', color: colors.textDim, cursor: 'pointer', padding: 4 }}
          >
            {collapsed ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
          </button>
        </div>
      </div>
      {!collapsed && (
        <>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {items.map((item, idx) => {
              const err = validationErrors?.get(idx);
              return (
                <div key={idx} style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <span style={{
                    color: colors.textMuted,
                    fontSize: 11,
                    fontFamily: font.mono,
                    width: 32,
                    textAlign: 'right',
                    flexShrink: 0,
                  }}>
                    {idx + 1}
                  </span>
                  <input
                    value={item}
                    onChange={(e) => onEdit(idx, e.target.value)}
                    style={{
                      ...inputStyle,
                      fontFamily: isMono ? font.mono : font.sans,
                      borderColor: err ? colors.critical : colors.border,
                    }}
                    placeholder={placeholder}
                  />
                  <button
                    onClick={() => onRemove(idx)}
                    style={btnDanger}
                    title="Remove"
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              );
            })}
            {items.length > 0 && validationErrors && validationErrors.size > 0 && (
              <div style={{ marginTop: 4 }}>
                {Array.from(validationErrors.entries()).map(([idx, err]) => (
                  <div key={idx} style={{ fontSize: 12, color: colors.critical, display: 'flex', gap: 4, alignItems: 'center', marginLeft: 40 }}>
                    <AlertTriangle size={12} />
                    Pattern {idx + 1}: {err}
                  </div>
                ))}
              </div>
            )}
          </div>
          <button
            onClick={onAdd}
            style={{ ...btnSmall, marginTop: 8, background: colors.bgSurface, color: colors.textDim, border: `1px solid ${colors.border}` }}
          >
            <Plus size={14} />
            Add {title.includes('Negative') ? 'Pattern' : 'Indicator'}
          </button>
        </>
      )}
    </div>
  );
}


export default function MatchingFiltersPage() {
  const [filters, setFilters] = useState<MatchingFilters>({ fraud_indicators: [], negative_patterns: [] });
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [dirty, setDirty] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [regexErrors, setRegexErrors] = useState<Map<number, string>>(new Map());

  // Test panel state
  const [testText, setTestText] = useState('');
  const [testResult, setTestResult] = useState<MatchingFiltersTestResult | null>(null);
  const [testing, setTesting] = useState(false);

  useEffect(() => {
    fetchMatchingFilters().then((data) => {
      setFilters(data);
      setLoading(false);
    }).catch(() => {
      setError('Failed to load matching filters');
      setLoading(false);
    });
  }, []);

  const validateRegex = useCallback((patterns: string[]) => {
    const errors = new Map<number, string>();
    patterns.forEach((pat, idx) => {
      if (!pat.trim()) return;
      try {
        new RegExp(pat, 'i');
      } catch (e) {
        errors.set(idx, (e as Error).message);
      }
    });
    setRegexErrors(errors);
    return errors.size === 0;
  }, []);

  const handleSave = async () => {
    if (!validateRegex(filters.negative_patterns)) {
      setError('Fix invalid regex patterns before saving');
      return;
    }

    // Remove empty strings
    const cleaned: MatchingFilters = {
      fraud_indicators: filters.fraud_indicators.filter(s => s.trim()),
      negative_patterns: filters.negative_patterns.filter(s => s.trim()),
    };

    setSaving(true);
    setError(null);
    setSuccess(null);
    try {
      const result = await updateMatchingFilters(cleaned);
      setFilters(result);
      setDirty(false);
      setSuccess(`Saved ${result.fraud_indicators.length} fraud indicators and ${result.negative_patterns.length} negative patterns. Matcher reloaded.`);
      setTimeout(() => setSuccess(null), 5000);
    } catch (e) {
      setError(`Failed to save: ${(e as Error).message}`);
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async () => {
    if (!testText.trim()) return;
    setTesting(true);
    setTestResult(null);
    try {
      const result = await testMatchingFilters(testText);
      setTestResult(result);
    } catch (e) {
      setError(`Test failed: ${(e as Error).message}`);
    } finally {
      setTesting(false);
    }
  };

  const update = (patch: Partial<MatchingFilters>) => {
    setFilters(f => ({ ...f, ...patch }));
    setDirty(true);
  };

  if (loading) {
    return (
      <div style={{ padding: '40px', color: colors.textDim }}>
        Loading matching filters...
      </div>
    );
  }

  return (
    <div>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <h1 style={{ fontSize: 24, fontWeight: 700, color: colors.text, margin: 0, display: 'flex', alignItems: 'center', gap: 10 }}>
            <Settings size={24} color={colors.accent} />
            Matching Filters
          </h1>
          <p style={{ fontSize: 14, color: colors.textDim, margin: '6px 0 0' }}>
            Manage noise reduction filters for the watch term matcher. Changes are applied immediately.
          </p>
        </div>
        <button
          onClick={handleSave}
          disabled={saving || !dirty}
          style={{
            ...btnStyle,
            opacity: (saving || !dirty) ? 0.5 : 1,
            cursor: (saving || !dirty) ? 'default' : 'pointer',
          }}
        >
          <Save size={16} />
          {saving ? 'Saving...' : 'Save Changes'}
        </button>
      </div>

      {/* Status messages */}
      {error && (
        <div style={{
          ...sectionStyle,
          background: 'rgba(239, 68, 68, 0.08)',
          borderColor: colors.critical,
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          color: colors.critical,
          fontSize: 14,
        }}>
          <XCircle size={16} />
          {error}
        </div>
      )}
      {success && (
        <div style={{
          ...sectionStyle,
          background: 'rgba(34, 197, 94, 0.08)',
          borderColor: colors.healthy,
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          color: colors.healthy,
          fontSize: 14,
        }}>
          <CheckCircle2 size={16} />
          {success}
        </div>
      )}

      {/* Dirty indicator */}
      {dirty && (
        <div style={{
          ...sectionStyle,
          background: 'rgba(234, 179, 8, 0.08)',
          borderColor: colors.medium,
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          color: colors.medium,
          fontSize: 13,
          padding: '10px 20px',
        }}>
          <AlertTriangle size={14} />
          You have unsaved changes.
        </div>
      )}

      {/* Two-column layout */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
        {/* Left column: Fraud Indicators */}
        <div>
          <PatternList
            title="Fraud Indicators"
            icon={<Shield size={16} color={colors.healthy} />}
            items={filters.fraud_indicators}
            onAdd={() => update({ fraud_indicators: [...filters.fraud_indicators, ''] })}
            onRemove={(idx) => update({ fraud_indicators: filters.fraud_indicators.filter((_, i) => i !== idx) })}
            onEdit={(idx, val) => {
              const next = [...filters.fraud_indicators];
              next[idx] = val;
              update({ fraud_indicators: next });
            }}
            placeholder="e.g. fullz, cvv, stealer"
          />
          <div style={{ ...sectionStyle, padding: '14px 20px', fontSize: 13, color: colors.textDim }}>
            <strong style={{ color: colors.text }}>How fraud indicators work:</strong>
            <br />
            At least one fraud indicator must appear alongside an institution name
            for a weak-signal match to be promoted to a finding. Case-insensitive substring match
            against the full mention text (title + content).
          </div>
        </div>

        {/* Right column: Negative Patterns */}
        <div>
          <PatternList
            title="Negative Patterns"
            icon={<ShieldOff size={16} color={colors.critical} />}
            items={filters.negative_patterns}
            onAdd={() => update({ negative_patterns: [...filters.negative_patterns, ''] })}
            onRemove={(idx) => {
              const next = filters.negative_patterns.filter((_, i) => i !== idx);
              update({ negative_patterns: next });
              validateRegex(next);
            }}
            onEdit={(idx, val) => {
              const next = [...filters.negative_patterns];
              next[idx] = val;
              update({ negative_patterns: next });
              validateRegex(next);
            }}
            placeholder="e.g. customer service, (?:best|worst) bank"
            isMono
            validationErrors={regexErrors}
          />
          <div style={{ ...sectionStyle, padding: '14px 20px', fontSize: 13, color: colors.textDim }}>
            <strong style={{ color: colors.text }}>How negative patterns work:</strong>
            <br />
            If any negative pattern matches the mention text, the mention is suppressed.
            Patterns are Python regex (case-insensitive). Use the test panel below to verify patterns.
          </div>
        </div>
      </div>

      {/* Test Panel */}
      <div style={{ ...sectionStyle, marginTop: 8 }}>
        <div style={sectionTitle}>
          <FlaskConical size={16} color={colors.accent} />
          Test Panel
        </div>
        <p style={{ fontSize: 13, color: colors.textDim, margin: '0 0 12px' }}>
          Paste sample text to see which patterns match or suppress it.
        </p>
        <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start' }}>
          <textarea
            value={testText}
            onChange={(e) => setTestText(e.target.value)}
            placeholder="Paste sample mention text here to test against current filters..."
            rows={4}
            style={{
              ...inputStyle,
              fontFamily: font.sans,
              resize: 'vertical',
              minHeight: 80,
            }}
          />
          <button
            onClick={handleTest}
            disabled={testing || !testText.trim()}
            style={{
              ...btnStyle,
              opacity: (testing || !testText.trim()) ? 0.5 : 1,
              cursor: (testing || !testText.trim()) ? 'default' : 'pointer',
              flexShrink: 0,
              alignSelf: 'flex-start',
            }}
          >
            <FlaskConical size={16} />
            {testing ? 'Testing...' : 'Test'}
          </button>
        </div>

        {testResult && (
          <div style={{ marginTop: 16 }}>
            {/* Verdict */}
            <div style={{
              display: 'flex',
              gap: 12,
              marginBottom: 12,
            }}>
              <div style={{
                ...tagStyle,
                background: testResult.would_suppress ? 'rgba(239, 68, 68, 0.12)' : 'rgba(34, 197, 94, 0.12)',
                borderColor: testResult.would_suppress ? colors.critical : colors.healthy,
                color: testResult.would_suppress ? colors.critical : colors.healthy,
                fontFamily: font.sans,
                fontWeight: 600,
              }}>
                {testResult.would_suppress ? <XCircle size={14} /> : <CheckCircle2 size={14} />}
                {testResult.would_suppress ? 'Would be SUPPRESSED (negative pattern matched)' : 'Not suppressed by negative patterns'}
              </div>
              <div style={{
                ...tagStyle,
                background: testResult.would_require_fraud_indicator ? 'rgba(234, 179, 8, 0.12)' : 'rgba(34, 197, 94, 0.12)',
                borderColor: testResult.would_require_fraud_indicator ? colors.medium : colors.healthy,
                color: testResult.would_require_fraud_indicator ? colors.medium : colors.healthy,
                fontFamily: font.sans,
                fontWeight: 600,
              }}>
                {testResult.would_require_fraud_indicator
                  ? <AlertTriangle size={14} />
                  : <CheckCircle2 size={14} />}
                {testResult.would_require_fraud_indicator
                  ? 'No fraud indicator found (weak signal would be filtered)'
                  : `${testResult.matched_fraud_indicators.length} fraud indicator(s) found`}
              </div>
            </div>

            {/* Matched negative patterns */}
            {testResult.matched_negative_patterns.length > 0 && (
              <div style={{ marginBottom: 12 }}>
                <div style={{ fontSize: 12, fontWeight: 600, color: colors.critical, marginBottom: 6 }}>
                  Matched Negative Patterns ({testResult.matched_negative_patterns.length}):
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                  {testResult.matched_negative_patterns.map((pat, i) => (
                    <span key={i} style={{
                      ...tagStyle,
                      background: 'rgba(239, 68, 68, 0.08)',
                      borderColor: colors.critical,
                      color: colors.critical,
                    }}>
                      {pat}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Matched fraud indicators */}
            {testResult.matched_fraud_indicators.length > 0 && (
              <div>
                <div style={{ fontSize: 12, fontWeight: 600, color: colors.healthy, marginBottom: 6 }}>
                  Matched Fraud Indicators ({testResult.matched_fraud_indicators.length}):
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                  {testResult.matched_fraud_indicators.map((ind, i) => (
                    <span key={i} style={{
                      ...tagStyle,
                      background: 'rgba(34, 197, 94, 0.08)',
                      borderColor: colors.healthy,
                      color: colors.healthy,
                    }}>
                      {ind}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
