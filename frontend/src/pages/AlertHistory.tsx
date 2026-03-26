import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { fetchNotifications, markNotificationRead, markAllNotificationsRead } from '../api';
import type { Notification } from '../types';
import { colors, card, font } from '../theme';
import { Bell, BellOff, CheckCheck, ExternalLink, ChevronLeft, Eye } from 'lucide-react';
import type { CSSProperties } from 'react';

const sectionStyle: CSSProperties = { ...card, marginBottom: 8 };

const btnStyle: CSSProperties = {
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
};

const btnSecondary: CSSProperties = {
  ...btnStyle,
  background: colors.bgSurface,
  color: colors.textDim,
  border: `1px solid ${colors.border}`,
};

const filterBtn = (active: boolean): CSSProperties => ({
  background: active ? 'rgba(99, 102, 241, 0.15)' : 'transparent',
  color: active ? colors.accent : colors.textDim,
  border: `1px solid ${active ? colors.accent : colors.border}`,
  borderRadius: 6,
  padding: '6px 14px',
  fontSize: 13,
  fontWeight: 500,
  cursor: 'pointer',
  fontFamily: font.sans,
});

export default function AlertHistoryPage() {
  const navigate = useNavigate();
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<'all' | 'unread'>('all');

  const load = async () => {
    try {
      const data = await fetchNotifications({
        unread_only: filter === 'unread' ? true : undefined,
        page_size: 100,
      });
      setNotifications(Array.isArray(data) ? data : []);
    } catch {
      setNotifications([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { setLoading(true); load(); }, [filter]);

  const handleMarkRead = async (id: string) => {
    try {
      await markNotificationRead(id);
      setNotifications(prev => prev.map(n => n.id === id ? { ...n, read: true } : n));
    } catch { /* ignore */ }
  };

  const handleMarkAllRead = async () => {
    try {
      await markAllNotificationsRead();
      setNotifications(prev => prev.map(n => ({ ...n, read: true })));
    } catch { /* ignore */ }
  };

  const unreadCount = notifications.filter(n => !n.read).length;

  if (loading) {
    return <div style={{ padding: 40, color: colors.textDim }}>Loading alert history...</div>;
  }

  return (
    <div>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
            <button
              onClick={() => navigate('/settings/alerts')}
              style={{ background: 'transparent', border: 'none', color: colors.textDim, cursor: 'pointer', padding: 0, display: 'flex' }}
            >
              <ChevronLeft size={20} />
            </button>
            <h1 style={{ fontSize: 24, fontWeight: 700, color: colors.text, margin: 0, display: 'flex', alignItems: 'center', gap: 10 }}>
              <Bell size={24} color={colors.accent} />
              Alert History
              {unreadCount > 0 && (
                <span style={{
                  background: colors.critical,
                  color: '#fff',
                  fontSize: 12,
                  fontWeight: 700,
                  borderRadius: 10,
                  padding: '2px 8px',
                  minWidth: 20,
                  textAlign: 'center',
                }}>
                  {unreadCount}
                </span>
              )}
            </h1>
          </div>
          <p style={{ fontSize: 14, color: colors.textDim, margin: '0 0 0 28px' }}>
            Notifications triggered by alert rules when findings match configured criteria.
          </p>
        </div>
        {unreadCount > 0 && (
          <button onClick={handleMarkAllRead} style={btnSecondary}>
            <CheckCheck size={16} /> Mark All Read
          </button>
        )}
      </div>

      {/* Filter */}
      <div style={{ display: 'flex', gap: 6, marginBottom: 16 }}>
        <button onClick={() => setFilter('all')} style={filterBtn(filter === 'all')}>
          All ({notifications.length})
        </button>
        <button onClick={() => setFilter('unread')} style={filterBtn(filter === 'unread')}>
          Unread {unreadCount > 0 ? `(${unreadCount})` : ''}
        </button>
      </div>

      {/* Notifications list */}
      {notifications.length === 0 ? (
        <div style={{ ...card, textAlign: 'center', padding: 40, color: colors.textDim }}>
          <BellOff size={32} style={{ marginBottom: 12, opacity: 0.4 }} />
          <div style={{ fontSize: 15, marginBottom: 8 }}>
            {filter === 'unread' ? 'No unread notifications' : 'No alert notifications yet'}
          </div>
          <div style={{ fontSize: 13 }}>
            Notifications appear here when findings match your alert rules.
          </div>
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {notifications.map(notif => (
            <div
              key={notif.id}
              style={{
                ...sectionStyle,
                borderLeft: `3px solid ${notif.read ? colors.border : colors.accent}`,
                background: notif.read ? colors.bgCard : 'rgba(99, 102, 241, 0.04)',
                display: 'flex',
                alignItems: 'flex-start',
                gap: 12,
                padding: '14px 16px',
              }}
            >
              {/* Unread dot */}
              {!notif.read && (
                <div style={{
                  width: 8,
                  height: 8,
                  borderRadius: '50%',
                  background: colors.accent,
                  marginTop: 5,
                  flexShrink: 0,
                }} />
              )}

              {/* Content */}
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 14, fontWeight: notif.read ? 400 : 600, color: colors.text, marginBottom: 4 }}>
                  {notif.title}
                </div>
                {notif.message && (
                  <div style={{ fontSize: 13, color: colors.textDim, marginBottom: 6, lineHeight: 1.4 }}>
                    {notif.message}
                  </div>
                )}
                <div style={{ display: 'flex', gap: 12, fontSize: 11, color: colors.textMuted }}>
                  <span>{new Date(notif.created_at).toLocaleString()}</span>
                  {notif.finding_id && (
                    <a
                      href={`/findings/${notif.finding_id}`}
                      onClick={e => { e.preventDefault(); navigate(`/findings/${notif.finding_id}`); }}
                      style={{ color: colors.accent, textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 4 }}
                    >
                      <ExternalLink size={10} /> View Finding
                    </a>
                  )}
                </div>
              </div>

              {/* Mark read */}
              {!notif.read && (
                <button
                  onClick={() => handleMarkRead(notif.id)}
                  title="Mark as read"
                  style={{
                    background: 'transparent',
                    border: 'none',
                    color: colors.textMuted,
                    cursor: 'pointer',
                    padding: 4,
                    flexShrink: 0,
                  }}
                >
                  <Eye size={14} />
                </button>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
