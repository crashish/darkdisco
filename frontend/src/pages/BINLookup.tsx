import { useState, useEffect } from 'react';
import { lookupBIN, searchBINs, fetchBINStats, importBINFile } from '../api';
import { colors, card, font } from '../theme';
import type { BINLookupResult, BINRecord, BINStats, BINImportResult } from '../types';
import { Search, CreditCard, Upload, Database, Globe, Building2, Hash } from 'lucide-react';
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
  padding: '10px 14px',
  color: colors.text,
  fontSize: 14,
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

const brandColors: Record<string, string> = {
  visa: '#1a1f71',
  mastercard: '#eb001b',
  amex: '#006fcf',
  discover: '#ff6600',
  jcb: '#0e4c96',
  unionpay: '#d0021b',
  diners: '#1a1f71',
  maestro: '#cc0000',
};

const brandBgColors: Record<string, string> = {
  visa: 'rgba(26, 31, 113, 0.15)',
  mastercard: 'rgba(235, 0, 27, 0.15)',
  amex: 'rgba(0, 111, 207, 0.15)',
  discover: 'rgba(255, 102, 0, 0.15)',
  jcb: 'rgba(14, 76, 150, 0.15)',
  unionpay: 'rgba(208, 2, 27, 0.15)',
  diners: 'rgba(26, 31, 113, 0.15)',
  maestro: 'rgba(204, 0, 0, 0.15)',
};

function BrandBadge({ brand }: { brand: string | null }) {
  if (!brand) return null;
  const color = brandColors[brand] || colors.textDim;
  const bg = brandBgColors[brand] || 'rgba(100, 116, 139, 0.12)';
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 4,
      padding: '2px 10px', borderRadius: 12, fontSize: 12, fontWeight: 600,
      color, background: bg, textTransform: 'uppercase',
    }}>
      <CreditCard size={12} />
      {brand}
    </span>
  );
}

function TypeBadge({ type }: { type: string | null }) {
  if (!type) return null;
  const typeColors: Record<string, string> = {
    credit: colors.accent, debit: colors.healthy,
    prepaid: colors.medium, charge: colors.high,
  };
  return (
    <span style={{
      display: 'inline-flex', padding: '2px 8px', borderRadius: 12,
      fontSize: 11, fontWeight: 500, color: typeColors[type] || colors.textDim,
      background: 'rgba(100, 116, 139, 0.1)',
    }}>
      {type}
    </span>
  );
}

export default function BINLookup() {
  const [lookupInput, setLookupInput] = useState('');
  const [lookupResult, setLookupResult] = useState<BINLookupResult | null>(null);
  const [lookupLoading, setLookupLoading] = useState(false);

  const [searchQuery, setSearchQuery] = useState('');
  const [searchBrand, setSearchBrand] = useState('');
  const [searchResults, setSearchResults] = useState<BINRecord[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);

  const [stats, setStats] = useState<BINStats | null>(null);

  const [importFile, setImportFile] = useState<File | null>(null);
  const [importLabel, setImportLabel] = useState('csv');
  const [importResult, setImportResult] = useState<BINImportResult | null>(null);
  const [importLoading, setImportLoading] = useState(false);

  useEffect(() => {
    fetchBINStats().then(setStats);
  }, []);

  const handleLookup = async () => {
    const prefix = lookupInput.replace(/\D/g, '').slice(0, 8);
    if (prefix.length < 6) return;
    setLookupLoading(true);
    try {
      const result = await lookupBIN(prefix);
      setLookupResult(result);
    } finally {
      setLookupLoading(false);
    }
  };

  const handleSearch = async () => {
    setSearchLoading(true);
    try {
      const results = await searchBINs({ q: searchQuery, brand: searchBrand || undefined, limit: 50 });
      setSearchResults(results);
    } finally {
      setSearchLoading(false);
    }
  };

  const handleImport = async () => {
    if (!importFile) return;
    setImportLoading(true);
    try {
      const result = await importBINFile(importFile, importLabel);
      setImportResult(result);
      fetchBINStats().then(setStats);
    } finally {
      setImportLoading(false);
    }
  };

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 700, color: colors.text, marginBottom: 24, display: 'flex', alignItems: 'center', gap: 10 }}>
        <CreditCard size={24} color={colors.accent} />
        BIN Database
      </h1>

      {/* Stats bar */}
      {stats && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 20 }}>
          <div style={sectionStyle}>
            <div style={{ fontSize: 11, color: colors.textMuted, textTransform: 'uppercase', marginBottom: 4 }}>Total Records</div>
            <div style={{ fontSize: 24, fontWeight: 700, color: colors.text }}>{stats.total_records.toLocaleString()}</div>
          </div>
          <div style={sectionStyle}>
            <div style={{ fontSize: 11, color: colors.textMuted, textTransform: 'uppercase', marginBottom: 4 }}>Card Brands</div>
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
              {Object.entries(stats.by_brand).slice(0, 5).map(([brand, count]) => (
                <span key={brand} style={{ fontSize: 11, color: colors.textDim }}>
                  <BrandBadge brand={brand} /> {count.toLocaleString()}
                </span>
              ))}
            </div>
          </div>
          <div style={sectionStyle}>
            <div style={{ fontSize: 11, color: colors.textMuted, textTransform: 'uppercase', marginBottom: 4 }}>Data Sources</div>
            <div style={{ fontSize: 14, color: colors.text }}>
              {Object.keys(stats.by_source).length} source{Object.keys(stats.by_source).length !== 1 ? 's' : ''}
            </div>
          </div>
          <div style={sectionStyle}>
            <div style={{ fontSize: 11, color: colors.textMuted, textTransform: 'uppercase', marginBottom: 4 }}>Countries</div>
            <div style={{ fontSize: 14, color: colors.text }}>
              {stats.by_country.length} countries
            </div>
          </div>
        </div>
      )}

      {/* BIN Lookup */}
      <div style={sectionStyle}>
        <div style={sectionTitle}>
          <Hash size={14} />
          BIN Lookup
        </div>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: 16 }}>
          <input
            style={{ ...inputStyle, maxWidth: 300, fontFamily: font.mono }}
            placeholder="Enter 6-8 digit BIN prefix..."
            value={lookupInput}
            onChange={e => setLookupInput(e.target.value.replace(/\D/g, '').slice(0, 8))}
            onKeyDown={e => e.key === 'Enter' && handleLookup()}
          />
          <button style={btnStyle} onClick={handleLookup} disabled={lookupLoading || lookupInput.length < 6}>
            <Search size={16} />
            {lookupLoading ? 'Looking up...' : 'Lookup'}
          </button>
        </div>

        {lookupResult && (
          <div style={{ background: colors.bgSurface, borderRadius: 8, padding: 16, border: `1px solid ${colors.border}` }}>
            {lookupResult.found ? (
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12 }}>
                  <span style={{ fontFamily: font.mono, fontSize: 18, fontWeight: 700, color: colors.text }}>
                    {lookupResult.bin_prefix}
                  </span>
                  <BrandBadge brand={lookupResult.card_brand} />
                  <TypeBadge type={lookupResult.card_type} />
                  {lookupResult.card_level && (
                    <span style={{ fontSize: 11, color: colors.textDim, textTransform: 'capitalize' }}>
                      {lookupResult.card_level}
                    </span>
                  )}
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                  {lookupResult.issuer_name && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                      <Building2 size={14} color={colors.textMuted} />
                      <span style={{ fontSize: 13, color: colors.textDim }}>Issuer:</span>
                      <span style={{ fontSize: 13, color: colors.text, fontWeight: 500 }}>{lookupResult.issuer_name}</span>
                    </div>
                  )}
                  {lookupResult.country_name && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                      <Globe size={14} color={colors.textMuted} />
                      <span style={{ fontSize: 13, color: colors.textDim }}>Country:</span>
                      <span style={{ fontSize: 13, color: colors.text }}>
                        {lookupResult.country_name}
                        {lookupResult.country_code && ` (${lookupResult.country_code})`}
                      </span>
                    </div>
                  )}
                  {lookupResult.bank_url && (
                    <div style={{ fontSize: 13, color: colors.textDim }}>
                      URL: <span style={{ color: colors.accent }}>{lookupResult.bank_url}</span>
                    </div>
                  )}
                  {lookupResult.bank_phone && (
                    <div style={{ fontSize: 13, color: colors.textDim }}>
                      Phone: <span style={{ color: colors.text }}>{lookupResult.bank_phone}</span>
                    </div>
                  )}
                </div>
              </div>
            ) : (
              <div style={{ color: colors.textMuted, fontSize: 14 }}>
                No record found for BIN prefix <strong style={{ color: colors.text }}>{lookupResult.bin_prefix}</strong>.
                Import BIN data below to populate the database.
              </div>
            )}
          </div>
        )}
      </div>

      {/* Search */}
      <div style={sectionStyle}>
        <div style={sectionTitle}>
          <Database size={14} />
          Search BIN Database
        </div>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: 16 }}>
          <input
            style={{ ...inputStyle, maxWidth: 300 }}
            placeholder="Search by issuer, prefix, or country..."
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleSearch()}
          />
          <select
            style={{ ...inputStyle, maxWidth: 160 }}
            value={searchBrand}
            onChange={e => setSearchBrand(e.target.value)}
          >
            <option value="">All Brands</option>
            <option value="visa">Visa</option>
            <option value="mastercard">Mastercard</option>
            <option value="amex">Amex</option>
            <option value="discover">Discover</option>
            <option value="jcb">JCB</option>
            <option value="unionpay">UnionPay</option>
          </select>
          <button style={btnStyle} onClick={handleSearch} disabled={searchLoading}>
            <Search size={16} />
            {searchLoading ? 'Searching...' : 'Search'}
          </button>
        </div>

        {searchResults.length > 0 && (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                  {['BIN', 'Brand', 'Type', 'Level', 'Issuer', 'Country', 'Source'].map(h => (
                    <th key={h} style={{ padding: '8px 12px', textAlign: 'left', color: colors.textMuted, fontWeight: 500, fontSize: 11, textTransform: 'uppercase' }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {searchResults.map(r => (
                  <tr key={r.id} style={{ borderBottom: `1px solid ${colors.border}` }}
                    onMouseEnter={e => (e.currentTarget.style.background = colors.bgHover)}
                    onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                  >
                    <td style={{ padding: '8px 12px', fontFamily: font.mono, fontWeight: 600, color: colors.text }}>{r.bin_prefix}</td>
                    <td style={{ padding: '8px 12px' }}><BrandBadge brand={r.card_brand} /></td>
                    <td style={{ padding: '8px 12px' }}><TypeBadge type={r.card_type} /></td>
                    <td style={{ padding: '8px 12px', color: colors.textDim, textTransform: 'capitalize' }}>{r.card_level || '-'}</td>
                    <td style={{ padding: '8px 12px', color: colors.text }}>{r.issuer_name || '-'}</td>
                    <td style={{ padding: '8px 12px', color: colors.textDim }}>
                      {r.country_name || '-'}
                      {r.country_code && ` (${r.country_code})`}
                    </td>
                    <td style={{ padding: '8px 12px', color: colors.textMuted }}>{r.source || '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Import */}
      <div style={sectionStyle}>
        <div style={sectionTitle}>
          <Upload size={14} />
          Import BIN Data
        </div>
        <p style={{ fontSize: 13, color: colors.textDim, marginBottom: 12 }}>
          Upload CSV or PDF files containing BIN data. Supports Visa/Mastercard BIN table PDFs and standard CSV formats.
        </p>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
          <input
            type="file"
            accept=".csv,.tsv,.pdf"
            onChange={e => setImportFile(e.target.files?.[0] || null)}
            style={{ fontSize: 13, color: colors.text }}
          />
          <input
            style={{ ...inputStyle, maxWidth: 200 }}
            placeholder="Source label (e.g., visa_2024)"
            value={importLabel}
            onChange={e => setImportLabel(e.target.value)}
          />
          <button style={btnStyle} onClick={handleImport} disabled={importLoading || !importFile}>
            <Upload size={16} />
            {importLoading ? 'Importing...' : 'Import'}
          </button>
        </div>

        {importResult && (
          <div style={{ marginTop: 16, background: colors.bgSurface, borderRadius: 8, padding: 16, border: `1px solid ${colors.border}` }}>
            <div style={{ display: 'flex', gap: 16, marginBottom: 8 }}>
              <span style={{ color: colors.healthy, fontWeight: 600 }}>{importResult.imported} imported</span>
              <span style={{ color: colors.accent }}>{importResult.updated} updated</span>
              <span style={{ color: colors.textMuted }}>{importResult.skipped} skipped</span>
            </div>
            {importResult.errors.length > 0 && (
              <div style={{ fontSize: 12, color: colors.critical }}>
                {importResult.errors.slice(0, 10).map((e, i) => (
                  <div key={i}>{e}</div>
                ))}
                {importResult.errors.length > 10 && (
                  <div style={{ color: colors.textMuted }}>...and {importResult.errors.length - 10} more errors</div>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Top Issuers */}
      {stats && stats.top_issuers.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionTitle}>
            <Building2 size={14} />
            Top Issuers
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(250px, 1fr))', gap: 8 }}>
            {stats.top_issuers.map((issuer, i) => (
              <div key={i} style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '8px 12px', borderRadius: 6, background: colors.bgSurface,
                border: `1px solid ${colors.border}`,
              }}>
                <span style={{ fontSize: 13, color: colors.text }}>{issuer.name}</span>
                <span style={{ fontSize: 12, color: colors.textMuted, fontFamily: font.mono }}>{issuer.count.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
