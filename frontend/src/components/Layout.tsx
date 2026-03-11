import { Outlet } from 'react-router-dom';
import Sidebar from './Sidebar';
import { colors, font } from '../theme';
import type { CSSProperties } from 'react';

const wrapperStyle: CSSProperties = {
  display: 'flex',
  minHeight: '100vh',
  background: colors.bg,
  color: colors.text,
  fontFamily: font.sans,
};

const mainStyle: CSSProperties = {
  marginLeft: 240,
  flex: 1,
  padding: '32px 40px',
  minWidth: 0,
};

export default function Layout() {
  return (
    <div style={wrapperStyle}>
      <Sidebar />
      <main style={mainStyle}>
        <Outlet />
      </main>
    </div>
  );
}
