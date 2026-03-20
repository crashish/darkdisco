"""Report generation engine — queries data, renders HTML, converts to PDF."""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from darkdisco.common.models import (
    Client,
    Finding,
    FindingStatus,
    Institution,
    Severity,
    Source,
)
from markupsafe import Markup, escape

from darkdisco.reporting import charts

logger = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).parent / "templates"

_SEVERITY_ORDER = ["critical", "high", "medium", "low", "info"]
_UNRESOLVED = {
    FindingStatus.new, FindingStatus.reviewing, FindingStatus.escalated, FindingStatus.confirmed,
}


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M")


def _fmt_date(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d")


async def _query_findings(
    session: AsyncSession,
    *,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    client_id: str | None = None,
    institution_id: str | None = None,
    severities: list[str] | None = None,
    statuses: list[str] | None = None,
) -> list[Finding]:
    """Query findings with filters, eager-loading relationships."""
    stmt = (
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .order_by(Finding.discovered_at.desc())
    )

    if date_from:
        stmt = stmt.where(Finding.discovered_at >= date_from)
    if date_to:
        stmt = stmt.where(Finding.discovered_at <= date_to)
    if institution_id:
        stmt = stmt.where(Finding.institution_id == institution_id)
    if client_id:
        stmt = stmt.join(Institution).where(Institution.client_id == client_id)
    if severities:
        stmt = stmt.where(Finding.severity.in_([Severity(s) for s in severities]))
    if statuses:
        stmt = stmt.where(Finding.status.in_([FindingStatus(s) for s in statuses]))

    result = await session.execute(stmt)
    return list(result.scalars().all())


async def _query_sources(session: AsyncSession) -> list[Source]:
    result = await session.execute(select(Source).order_by(Source.name))
    return list(result.scalars().all())


async def _query_institutions(
    session: AsyncSession,
    findings: list[Finding],
) -> list[dict]:
    """Build institution exposure data from findings."""
    inst_data: dict[str, dict] = {}
    for f in findings:
        iid = f.institution_id
        if iid not in inst_data:
            inst_data[iid] = {
                "name": f.institution_name or "Unknown",
                "client_name": "",
                "total": 0,
                "critical": 0,
                "high": 0,
                "unresolved": 0,
            }
        inst_data[iid]["total"] += 1
        if f.severity == Severity.critical:
            inst_data[iid]["critical"] += 1
        elif f.severity == Severity.high:
            inst_data[iid]["high"] += 1
        if f.status in _UNRESOLVED:
            inst_data[iid]["unresolved"] += 1

    # Fetch client names
    if inst_data:
        stmt = (
            select(Institution)
            .options(selectinload(Institution.client))
            .where(Institution.id.in_(list(inst_data.keys())))
        )
        result = await session.execute(stmt)
        for inst in result.scalars().all():
            if inst.id in inst_data:
                inst_data[inst.id]["client_name"] = inst.client.name if inst.client else ""

    return sorted(inst_data.values(), key=lambda x: x["total"], reverse=True)


_CONTEXT_CHARS = 120  # chars of context around each highlight match


def _highlight_content(raw_content: str, matched_terms: list[dict]) -> Markup:
    """Build a truncated excerpt of raw_content with <mark> tags around matches.

    Shows a context window around each match rather than the full content,
    joining non-contiguous segments with ellipsis. Keeps report concise.
    """
    # Collect all highlight spans
    spans: list[tuple[int, int]] = []
    for term in matched_terms:
        for hl in term.get("highlights", []):
            start = hl.get("start", 0)
            end = hl.get("end", 0)
            if 0 <= start < end <= len(raw_content):
                spans.append((start, end))

    if not spans:
        # No highlights — just show a truncated preview
        preview = raw_content[:400]
        if len(raw_content) > 400:
            preview += "…"
        return Markup(escape(preview))

    # Merge overlapping spans
    spans.sort()
    merged: list[tuple[int, int]] = [spans[0]]
    for start, end in spans[1:]:
        prev_start, prev_end = merged[-1]
        if start <= prev_end:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))

    # Build context windows around each match
    windows: list[tuple[int, int]] = []
    for start, end in merged:
        win_start = max(0, start - _CONTEXT_CHARS)
        win_end = min(len(raw_content), end + _CONTEXT_CHARS)
        windows.append((win_start, win_end))

    # Merge overlapping windows
    merged_windows: list[tuple[int, int]] = [windows[0]]
    for win_start, win_end in windows[1:]:
        prev_start, prev_end = merged_windows[-1]
        if win_start <= prev_end:
            merged_windows[-1] = (prev_start, max(prev_end, win_end))
        else:
            merged_windows.append((win_start, win_end))

    # Build output: excerpt with highlights, ellipsis between segments
    parts: list[str] = []
    for i, (win_start, win_end) in enumerate(merged_windows):
        if i == 0 and win_start > 0:
            parts.append("…")
        elif i > 0:
            parts.append(" … ")

        segment = raw_content[win_start:win_end]
        # Apply highlight marks within this segment
        seg_parts: list[str] = []
        seg_pos = win_start
        for mark_start, mark_end in merged:
            if mark_end <= win_start or mark_start >= win_end:
                continue
            # Clamp to window
            ms = max(mark_start, win_start)
            me = min(mark_end, win_end)
            # Text before this mark
            if ms > seg_pos:
                seg_parts.append(str(escape(raw_content[seg_pos:ms])))
            seg_parts.append("<mark>")
            seg_parts.append(str(escape(raw_content[ms:me])))
            seg_parts.append("</mark>")
            seg_pos = me
        # Remaining text in window
        if seg_pos < win_end:
            seg_parts.append(str(escape(raw_content[seg_pos:win_end])))
        parts.append("".join(seg_parts))

        if win_end < len(raw_content) and i == len(merged_windows) - 1:
            parts.append("…")

    return Markup("".join(parts))


def _build_finding_dicts(findings: list[Finding]) -> list[dict]:
    """Convert Finding ORM objects to dicts for template and chart use."""
    result = []
    for f in findings:
        matched_terms = f.matched_terms or []
        # Extract unique term values for display
        term_values = list(dict.fromkeys(t.get("value", "") for t in matched_terms if t.get("value")))
        # Build highlighted content if raw_content and highlights exist
        highlighted_content = None
        if f.raw_content and matched_terms:
            highlighted_content = _highlight_content(f.raw_content, matched_terms)

        result.append({
            "id": f.id,
            "title": f.title,
            "severity": f.severity.value if isinstance(f.severity, Severity) else str(f.severity),
            "status": f.status.value if isinstance(f.status, FindingStatus) else str(f.status),
            "institution_name": f.institution_name,
            "source_type": f.source_type,
            "source_name": f.source_name,
            "summary": f.summary,
            "classification": f.classification,
            "matched_terms": term_values,
            "highlighted_content": highlighted_content,
            "discovered_at": f.discovered_at,
            "discovered_at_fmt": _fmt_dt(f.discovered_at),
        })
    return result


def _build_severity_groups(finding_dicts: list[dict]) -> list[dict]:
    """Group findings by severity for detailed view."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for f in finding_dicts:
        groups[f["severity"]].append(f)
    return [
        {"label": sev.title(), "severity": sev, "findings": groups[sev]}
        for sev in _SEVERITY_ORDER
        if groups.get(sev)
    ]


def _build_classifications(finding_dicts: list[dict]) -> list[dict]:
    """Build classification breakdown."""
    counts = Counter(f.get("classification") or "Unclassified" for f in finding_dicts)
    total = len(finding_dicts) or 1
    return sorted(
        [
            {"label": cls, "count": cnt, "pct": round(cnt / total * 100, 1)}
            for cls, cnt in counts.items()
        ],
        key=lambda x: x["count"],
        reverse=True,
    )


def _build_timeline(finding_dicts: list[dict]) -> list[dict]:
    """Build daily timeline of finding activity."""
    daily: dict[str, dict] = defaultdict(lambda: {"new": 0, "critical_high": 0, "resolved": 0})
    for f in finding_dicts:
        dt = f.get("discovered_at")
        if isinstance(dt, datetime):
            day = dt.strftime("%Y-%m-%d")
        else:
            continue
        daily[day]["new"] += 1
        if f["severity"] in ("critical", "high"):
            daily[day]["critical_high"] += 1
        if f["status"] in ("resolved", "dismissed", "false_positive"):
            daily[day]["resolved"] += 1

    return [{"date": d, **daily[d]} for d in sorted(daily.keys())]


def _build_stats(finding_dicts: list[dict], institutions: list[dict], sources: list) -> dict:
    """Build executive summary stats."""
    sev_counts = Counter(f["severity"] for f in finding_dicts)
    total = len(finding_dicts)
    critical = sev_counts.get("critical", 0)
    high = sev_counts.get("high", 0)

    summary_parts = []
    if total:
        summary_parts.append(f"This report covers {total} findings")
    if critical + high > 0:
        summary_parts.append(
            f"of which {critical + high} are critical or high severity"
        )
    if institutions:
        summary_parts.append(f"across {len(institutions)} affected institutions")
    summary_text = ", ".join(summary_parts) + "." if summary_parts else ""

    return {
        "total_findings": total,
        "critical_count": critical,
        "high_count": high,
        "institutions_affected": len(institutions),
        "sources_active": len([s for s in sources if getattr(s, "enabled", True)]),
        "summary_text": summary_text,
    }


def _generate_charts(finding_dicts: list[dict], include: dict) -> dict:
    """Generate requested charts. Returns dict of chart_name -> base64 PNG."""
    result = {}
    if include.get("severity_pie", True):
        result["severity_pie"] = charts.severity_pie(finding_dicts)
    if include.get("status_pie", True):
        result["status_pie"] = charts.status_pie(finding_dicts)
    if include.get("trend_line", True):
        result["trend_line"] = charts.trend_line(finding_dicts)
    if include.get("source_bar", True):
        result["source_bar"] = charts.source_bar(finding_dicts)
    if include.get("institution_bar", True):
        result["institution_bar"] = charts.institution_bar(finding_dicts)
    if include.get("severity_trend", True):
        result["severity_trend"] = charts.severity_trend(finding_dicts)
    return {k: v for k, v in result.items() if v}


async def render_report_html(
    session: AsyncSession,
    *,
    title: str = "DarkDisco Threat Intelligence Report",
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    client_id: str | None = None,
    institution_id: str | None = None,
    severities: list[str] | None = None,
    statuses: list[str] | None = None,
    sections: dict | None = None,
    chart_options: dict | None = None,
) -> str:
    """Build the full report HTML from data and template.

    Args:
        session: async DB session
        title: report title
        date_from/date_to: date range filter
        client_id/institution_id: scope filter
        severities/statuses: finding filters
        sections: dict of section_name -> bool for toggle
        chart_options: dict of chart_name -> bool for toggle

    Returns:
        Rendered HTML string.
    """
    # Defaults: all sections on
    sec = {
        "executive_summary": True,
        "charts": True,
        "findings_detail": True,
        "findings_by_severity": True,
        "source_activity": True,
        "institution_exposure": True,
        "classification_breakdown": True,
        "timeline": True,
    }
    if sections:
        sec.update(sections)

    chart_inc = chart_options or {}

    # Query data
    findings = await _query_findings(
        session,
        date_from=date_from,
        date_to=date_to,
        client_id=client_id,
        institution_id=institution_id,
        severities=severities,
        statuses=statuses,
    )
    finding_dicts = _build_finding_dicts(findings)

    sources_list = await _query_sources(session) if sec["source_activity"] else []
    source_data = [
        {
            "name": s.name,
            "source_type": s.source_type.value if hasattr(s.source_type, "value") else str(s.source_type),
            "finding_count": 0,
            "last_polled_fmt": _fmt_dt(s.last_polled_at),
            "enabled": s.enabled,
        }
        for s in sources_list
    ]
    # Count findings per source
    source_counts = Counter(f.source_name for f in findings if f.source_name)
    for sd in source_data:
        sd["finding_count"] = source_counts.get(sd["name"], 0)

    inst_data = await _query_institutions(session, findings) if sec["institution_exposure"] else []
    stats = _build_stats(finding_dicts, inst_data, sources_list)
    severity_groups = _build_severity_groups(finding_dicts) if sec.get("findings_by_severity") else []
    classifications = _build_classifications(finding_dicts) if sec["classification_breakdown"] else []
    timeline = _build_timeline(finding_dicts) if sec["timeline"] else []
    chart_data = _generate_charts(finding_dicts, chart_inc) if sec["charts"] else {}

    # Date range label
    parts = []
    if date_from:
        parts.append(f"From: {_fmt_date(date_from)}")
    if date_to:
        parts.append(f"To: {_fmt_date(date_to)}")
    date_range_label = " — ".join(parts) if parts else "All Time"

    # Client name
    client_name = None
    if client_id:
        result = await session.execute(select(Client).where(Client.id == client_id))
        client = result.scalar_one_or_none()
        if client:
            client_name = client.name

    # Render template
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATE_DIR)),
        autoescape=True,
    )
    template = env.get_template("report.html")
    return template.render(
        title=title,
        date_range_label=date_range_label,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        client_name=client_name,
        sections=sec,
        stats=stats,
        charts=chart_data,
        findings=finding_dicts,
        severity_groups=severity_groups,
        sources=source_data,
        institutions=inst_data,
        classifications=classifications,
        timeline=timeline,
    )


async def generate_pdf(
    session: AsyncSession,
    **kwargs,
) -> bytes:
    """Generate a PDF report. Returns PDF bytes.

    Accepts same kwargs as render_report_html.
    """
    import weasyprint

    html = await render_report_html(session, **kwargs)
    pdf_bytes = weasyprint.HTML(string=html).write_pdf()
    return pdf_bytes
