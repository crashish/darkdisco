import { NavLink } from 'react-router-dom';
import { LayoutDashboard, Search, Building2, Radio, Disc3 } from 'lucide-react';
import { colors, font } from '../theme';
import type { CSSProperties } from 'react';

const navItems = [
  { to: '/', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/findings', icon: Search, label: 'Findings' },
  { to: '/institutions', icon: Building2, label: 'Institutions' },
  { to: '/sources', icon: Radio, label: 'Sources' },
];

const sidebarStyle: CSSProperties = {
  width: 240,
  minHeight: '100vh',
  background: colors.bgCard,
  borderRight: `1px solid ${colors.border}`,
  display: 'flex',
  flexDirection: 'column',
  padding: '0',
  position: 'fixed',
  top: 0,
  left: 0,
  bottom: 0,
  zIndex: 100,
};

const brandStyle: CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 10,
  padding: '24px 20px',
  borderBottom: `1px solid ${colors.border}`,
  fontSize: 20,
  fontWeight: 700,
  fontFamily: font.sans,
  color: colors.text,
  letterSpacing: '-0.02em',
};

const navStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  padding: '12px 8px',
  flex: 1,
};

const linkBase: CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 12,
  padding: '10px 12px',
  borderRadius: 6,
  fontSize: 14,
  fontWeight: 500,
  color: colors.textDim,
  textDecoration: 'none',
  transition: 'all 0.15s',
};

const linkActive: CSSProperties = {
  ...linkBase,
  color: colors.text,
  background: colors.bgSurface,
};

export default function Sidebar() {
  return (
    <aside style={sidebarStyle}>
      <div style={brandStyle}>
        <Disc3 size={24} color={colors.accent} />
        DarkDisco
      </div>
      <nav style={navStyle}>
        {navItems.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            end={to === '/'}
            style={({ isActive }) => isActive ? linkActive : linkBase}
            onMouseEnter={e => {
              if (!e.currentTarget.classList.contains('active')) {
                (e.currentTarget as HTMLElement).style.background = colors.bgHover;
                (e.currentTarget as HTMLElement).style.color = colors.text;
              }
            }}
            onMouseLeave={e => {
              if (!e.currentTarget.classList.contains('active')) {
                (e.currentTarget as HTMLElement).style.background = 'transparent';
                (e.currentTarget as HTMLElement).style.color = colors.textDim;
              }
            }}
          >
            <Icon size={18} />
            {label}
          </NavLink>
        ))}
      </nav>
      <div style={{ padding: '16px 20px', borderTop: `1px solid ${colors.border}`, fontSize: 11, color: colors.textMuted }}>
        DarkDisco v0.1.0
      </div>
    </aside>
  );
}
