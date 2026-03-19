import { useEffect, useState, useRef } from 'react';
import { colors } from '../theme';
import { ChevronDown } from 'lucide-react';

export default function MultiSelect({ label, options, selected, onChange }: {
  label: string;
  options: { value: string; label: string }[];
  selected: Set<string>;
  onChange: (next: Set<string>) => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const toggle = (val: string) => {
    const next = new Set(selected);
    if (next.has(val)) next.delete(val); else next.add(val);
    onChange(next);
  };

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          padding: '7px 10px', fontSize: 13, background: colors.bgSurface,
          border: `1px solid ${selected.size > 0 ? colors.accent : colors.border}`,
          borderRadius: 6, color: colors.text, cursor: 'pointer',
          display: 'inline-flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap',
        }}
      >
        {label}
        {selected.size > 0 && (
          <span style={{
            background: colors.accent, color: '#fff', fontSize: 10, fontWeight: 700,
            borderRadius: 9999, minWidth: 18, height: 18, display: 'inline-flex',
            alignItems: 'center', justifyContent: 'center', padding: '0 5px',
          }}>
            {selected.size}
          </span>
        )}
        <ChevronDown size={12} />
      </button>
      {open && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, marginTop: 4, zIndex: 50,
          background: colors.bgCard, border: `1px solid ${colors.border}`, borderRadius: 6,
          boxShadow: '0 8px 24px rgba(0,0,0,0.4)', minWidth: 220, maxHeight: 280, overflow: 'auto',
        }}>
          {selected.size > 0 && (
            <button
              onClick={() => onChange(new Set())}
              style={{
                width: '100%', padding: '6px 12px', fontSize: 11, color: colors.accent,
                background: 'none', border: 'none', borderBottom: `1px solid ${colors.border}`,
                cursor: 'pointer', textAlign: 'left',
              }}
            >
              Clear all
            </button>
          )}
          {options.map(opt => (
            <label
              key={opt.value}
              style={{
                display: 'flex', alignItems: 'center', gap: 8, padding: '6px 12px',
                fontSize: 12, color: colors.text, cursor: 'pointer',
                background: selected.has(opt.value) ? colors.bgHover : 'transparent',
              }}
            >
              <input
                type="checkbox"
                checked={selected.has(opt.value)}
                onChange={() => toggle(opt.value)}
                style={{ accentColor: colors.accent }}
              />
              <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {opt.label}
              </span>
            </label>
          ))}
          {options.length === 0 && (
            <div style={{ padding: '8px 12px', fontSize: 11, color: colors.textMuted }}>No options</div>
          )}
        </div>
      )}
    </div>
  );
}
