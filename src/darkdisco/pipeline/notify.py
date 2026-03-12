"""Notification delivery — email (SMTP), Slack webhook, and generic webhook.

Called from evaluate_alerts after a Notification row is persisted. Each delivery
channel is fire-and-forget with logging; failures don't block other channels.
"""

from __future__ import annotations

import logging
import smtplib
import ssl
from email.message import EmailMessage
from typing import Any

import httpx

from darkdisco.config import settings

logger = logging.getLogger(__name__)


def deliver_notification(
    *,
    rule: Any,
    title: str,
    message: str,
    finding_id: str | None = None,
) -> dict[str, bool]:
    """Deliver a notification across all channels enabled on the alert rule.

    Returns a dict of {channel: success} for auditing.
    """
    results: dict[str, bool] = {}

    if rule.notify_email:
        results["email"] = _send_email(
            to_address=_resolve_email(rule.owner_id),
            subject=title,
            body=message,
        )

    if rule.notify_slack:
        results["slack"] = _send_slack(title=title, message=message, finding_id=finding_id)

    if rule.notify_webhook_url:
        results["webhook"] = _send_webhook(
            url=rule.notify_webhook_url,
            payload={
                "title": title,
                "message": message,
                "finding_id": finding_id,
                "alert_rule_id": rule.id,
            },
        )

    return results


# ---------------------------------------------------------------------------
# Email (SMTP)
# ---------------------------------------------------------------------------


def _resolve_email(user_id: str) -> str:
    """Look up a user's email address. Falls back to settings default."""
    # Users table doesn't have an email column yet — use the configured
    # fallback address. A future migration can add user.email.
    return settings.smtp_default_recipient


def _send_email(*, to_address: str, subject: str, body: str) -> bool:
    if not settings.smtp_host:
        logger.warning("SMTP not configured — skipping email notification")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = settings.smtp_from_address
    msg["To"] = to_address
    msg.set_content(body)

    try:
        if settings.smtp_use_tls:
            ctx = ssl.create_default_context()
            with smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port, context=ctx) as srv:
                if settings.smtp_username:
                    srv.login(settings.smtp_username, settings.smtp_password)
                srv.send_message(msg)
        else:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as srv:
                if settings.smtp_use_starttls:
                    srv.starttls(context=ssl.create_default_context())
                if settings.smtp_username:
                    srv.login(settings.smtp_username, settings.smtp_password)
                srv.send_message(msg)
        logger.info("Email sent to %s: %s", to_address, subject)
        return True
    except Exception:
        logger.exception("Failed to send email to %s", to_address)
        return False


# ---------------------------------------------------------------------------
# Slack webhook
# ---------------------------------------------------------------------------


def _send_slack(*, title: str, message: str, finding_id: str | None) -> bool:
    if not settings.slack_webhook_url:
        logger.warning("Slack webhook not configured — skipping Slack notification")
        return False

    payload = {
        "text": f"*{title}*\n{message}",
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{title}*\n{message}"},
            },
        ],
    }

    try:
        resp = httpx.post(
            settings.slack_webhook_url,
            json=payload,
            timeout=10.0,
        )
        resp.raise_for_status()
        logger.info("Slack notification sent: %s", title)
        return True
    except Exception:
        logger.exception("Failed to send Slack notification")
        return False


# ---------------------------------------------------------------------------
# Generic webhook
# ---------------------------------------------------------------------------


def _send_webhook(*, url: str, payload: dict) -> bool:
    try:
        resp = httpx.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json", "User-Agent": "DarkDisco/1.0"},
            timeout=10.0,
        )
        resp.raise_for_status()
        logger.info("Webhook delivered to %s", url)
        return True
    except Exception:
        logger.exception("Failed to deliver webhook to %s", url)
        return False
