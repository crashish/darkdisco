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
    RawMention,
    Severity,
    Source,
)
from sqlalchemy import func
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


def _highlight_content(raw_content: str, matched_terms: list[dict], *, truncate: bool = True) -> Markup:
    """Build content with <mark> tags around matches.

    When truncate=True, shows context windows around each match with ellipsis
    between non-contiguous segments. When truncate=False, shows full content.
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

    # Full content mode: highlight matches in the complete text
    if not truncate:
        parts: list[str] = []
        pos = 0
        for start, end in merged:
            parts.append(str(escape(raw_content[pos:start])))
            parts.append("<mark>")
            parts.append(str(escape(raw_content[start:end])))
            parts.append("</mark>")
            pos = end
        parts.append(str(escape(raw_content[pos:])))
        return Markup("".join(parts))

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


def _build_finding_dicts(findings: list[Finding], *, truncate: bool = True) -> list[dict]:
    """Convert Finding ORM objects to dicts for template and chart use."""
    result = []
    for f in findings:
        matched_terms = f.matched_terms or []
        # Extract unique term values for display
        term_values = list(dict.fromkeys(t.get("value", "") for t in matched_terms if t.get("value")))
        # Build highlighted content if raw_content and highlights exist
        highlighted_content = None
        full_content = None
        if f.raw_content and matched_terms:
            highlighted_content = _highlight_content(f.raw_content, matched_terms, truncate=truncate)
            if truncate:
                full_content = _highlight_content(f.raw_content, matched_terms, truncate=False)
            else:
                full_content = highlighted_content
        elif f.raw_content:
            full_content = Markup(escape(f.raw_content))

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
            "analyst_notes": f.analyst_notes,
            "matched_terms": term_values,
            "highlighted_content": highlighted_content,
            "full_content": full_content,
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


def _build_fp_analytics(findings: list[Finding]) -> dict:
    """Build FP analytics: rates by institution, disposition breakdown, noise metrics."""
    inst_agg: dict[str, dict] = {}
    for f in findings:
        iid = f.institution_id
        if iid not in inst_agg:
            inst_agg[iid] = {
                "institution_id": iid,
                "institution_name": f.institution_name or "Unknown",
                "total_findings": 0,
                "false_positives": 0,
                "dismissed": 0,
                "confirmed": 0,
            }
        inst_agg[iid]["total_findings"] += 1
        if f.status == FindingStatus.false_positive:
            inst_agg[iid]["false_positives"] += 1
        elif f.status == FindingStatus.dismissed:
            inst_agg[iid]["dismissed"] += 1
        elif f.status == FindingStatus.confirmed:
            inst_agg[iid]["confirmed"] += 1

    institution_fp_rates = []
    for d in sorted(inst_agg.values(), key=lambda x: x["total_findings"], reverse=True):
        noise = d["false_positives"] + d["dismissed"]
        fp_rate = noise / d["total_findings"] if d["total_findings"] > 0 else 0.0
        institution_fp_rates.append({**d, "fp_rate": round(fp_rate, 4)})

    # Overall disposition breakdown
    status_counts = Counter(
        f.status.value if isinstance(f.status, FindingStatus) else str(f.status)
        for f in findings
    )
    disposition_breakdown = [
        {"status": s, "count": c}
        for s, c in status_counts.most_common()
    ]

    # Noise reduction metrics
    total = len(findings)
    noise = sum(1 for f in findings if f.status in {FindingStatus.false_positive, FindingStatus.dismissed})
    noise_rate = noise / total if total > 0 else 0.0

    return {
        "institution_fp_rates": institution_fp_rates,
        "disposition_breakdown": disposition_breakdown,
        "total_findings": total,
        "total_noise": noise,
        "noise_rate": round(noise_rate, 4),
        "noise_rate_pct": round(noise_rate * 100, 1),
    }


async def _build_pattern_effectiveness(
    session: AsyncSession,
    *,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    institution_id: str | None = None,
) -> dict:
    """Build pattern effectiveness stats: mentions, suppression rate, top patterns."""
    filters = []
    if date_from:
        filters.append(RawMention.collected_at >= date_from)
    if date_to:
        filters.append(RawMention.collected_at <= date_to)

    total_q = select(func.count(RawMention.id)).where(*filters) if filters else select(func.count(RawMention.id))
    total_mentions = (await session.execute(total_q)).scalar() or 0

    promoted_filters = [RawMention.promoted_to_finding_id.isnot(None)] + filters
    promoted_q = select(func.count(RawMention.id)).where(*promoted_filters)
    total_promoted = (await session.execute(promoted_q)).scalar() or 0

    total_suppressed = total_mentions - total_promoted
    suppression_rate = total_suppressed / total_mentions if total_mentions > 0 else 0.0

    # FP score distribution from finding metadata
    fp_buckets = {"0.0-0.2": 0, "0.2-0.4": 0, "0.4-0.6": 0, "0.6-0.8": 0, "0.8-1.0": 0}
    fp_filters = [Finding.metadata_["false_positive"]["fp_score"].isnot(None)]
    if date_from:
        fp_filters.append(Finding.discovered_at >= date_from)
    if date_to:
        fp_filters.append(Finding.discovered_at <= date_to)
    if institution_id:
        fp_filters.append(Finding.institution_id == institution_id)

    try:
        fp_score_q = select(
            Finding.metadata_["false_positive"]["fp_score"].as_float()
        ).where(*fp_filters)
        fp_scores = (await session.execute(fp_score_q)).scalars().all()
        for score in fp_scores:
            if score is None:
                continue
            s = float(score)
            if s < 0.2:
                fp_buckets["0.0-0.2"] += 1
            elif s < 0.4:
                fp_buckets["0.2-0.4"] += 1
            elif s < 0.6:
                fp_buckets["0.4-0.6"] += 1
            elif s < 0.8:
                fp_buckets["0.6-0.8"] += 1
            else:
                fp_buckets["0.8-1.0"] += 1
    except Exception:
        logger.debug("FP score query failed (metadata may not have expected structure)")

    return {
        "total_mentions": total_mentions,
        "total_promoted": total_promoted,
        "total_suppressed": total_suppressed,
        "suppression_rate": round(suppression_rate, 4),
        "suppression_rate_pct": round(suppression_rate * 100, 1),
        "fp_score_distribution": [{"bucket": k, "count": v} for k, v in fp_buckets.items()],
    }


def _classify_finding_for_report(
    tags: list | None,
    matched_terms: list | None,
    title: str,
    severity: Severity,
) -> list[str]:
    """Derive threat categories from finding attributes."""
    cats: set[str] = set()
    text = (title or "").lower()
    tag_set = {t.lower() for t in (tags or []) if isinstance(t, str)}
    term_values = {t.get("value", "").lower() for t in (matched_terms or []) if isinstance(t, dict)}

    all_text = text + " " + " ".join(tag_set) + " " + " ".join(term_values)

    if any(k in all_text for k in ("card", "bin", "cvv", "cc ", "fullz", "dump")):
        cats.add("Card Fraud")
    if any(k in all_text for k in ("phish", "spoof", "typosquat")):
        cats.add("Phishing")
    if any(k in all_text for k in ("ato", "account takeover", "credential", "combo", "login")):
        cats.add("Account Takeover")
    if any(k in all_text for k in ("leak", "breach", "exposed", "database")):
        cats.add("Data Breach")
    if any(k in all_text for k in ("stealer", "infostealer", "redline", "raccoon", "vidar")):
        cats.add("Credential Leaks")
    if any(k in all_text for k in ("ransom", "extort", "lockbit", "blackcat")):
        cats.add("Ransomware")
    if not cats:
        cats.add("Other")
    return sorted(cats)


async def _build_institution_threat_summaries(
    findings: list[Finding],
) -> list[dict]:
    """Build per-institution threat summaries from findings."""
    inst_groups: dict[str, list[Finding]] = defaultdict(list)
    for f in findings:
        inst_groups[f.institution_id].append(f)

    summaries = []
    for iid, inst_findings in inst_groups.items():
        name = inst_findings[0].institution_name or "Unknown"
        total = len(inst_findings)
        confirmed = sum(
            1 for f in inst_findings
            if f.status in {FindingStatus.confirmed, FindingStatus.escalated}
        )

        # Severity breakdown
        sev_counts = Counter(
            f.severity.value if isinstance(f.severity, Severity) else str(f.severity)
            for f in inst_findings
        )

        # Threat categories
        category_counts: dict[str, int] = {}
        threat_actors: set[str] = set()
        for f in inst_findings:
            cats = _classify_finding_for_report(f.tags, f.matched_terms, f.title, f.severity)
            for cat in cats:
                category_counts[cat] = category_counts.get(cat, 0) + 1
            if f.tags:
                for tag in f.tags:
                    if isinstance(tag, str) and tag.startswith("actor:"):
                        threat_actors.add(tag[6:])

        threat_categories = sorted(
            [{"category": k, "count": v} for k, v in category_counts.items()],
            key=lambda x: x["count"],
            reverse=True,
        )

        # Timeline (daily)
        daily: dict[str, int] = defaultdict(int)
        for f in inst_findings:
            if isinstance(f.discovered_at, datetime):
                daily[f.discovered_at.strftime("%Y-%m-%d")] += 1
        timeline = [{"date": d, "count": daily[d]} for d in sorted(daily.keys())]

        summaries.append({
            "institution_id": iid,
            "institution_name": name,
            "total_findings": total,
            "confirmed_threats": confirmed,
            "active_threat_actors": len(threat_actors),
            "by_severity": dict(sev_counts),
            "threat_categories": threat_categories,
            "timeline": timeline,
        })

    return sorted(summaries, key=lambda x: x["total_findings"], reverse=True)


async def _build_analyst_performance(
    findings: list[Finding],
) -> dict:
    """Build analyst performance metrics: throughput, disposition time, escalation."""
    # Reviewed findings with timing
    reviewed = [f for f in findings if f.reviewed_at and f.reviewed_by]
    pending_statuses = {FindingStatus.new, FindingStatus.reviewing}
    pending = [f for f in findings if f.status in pending_statuses]

    # By analyst
    analyst_stats: dict[str, dict] = {}
    for f in reviewed:
        name = f.reviewed_by or "unknown"
        if name not in analyst_stats:
            analyst_stats[name] = {"analyst": name, "reviewed": 0, "pending": 0, "total_hours": 0.0}
        analyst_stats[name]["reviewed"] += 1
        if f.reviewed_at and f.created_at:
            hours = (f.reviewed_at - f.created_at).total_seconds() / 3600
            analyst_stats[name]["total_hours"] += hours

    # Add pending counts
    for f in pending:
        name = f.assigned_to or "unassigned"
        if name not in analyst_stats:
            analyst_stats[name] = {"analyst": name, "reviewed": 0, "pending": 0, "total_hours": 0.0}
        analyst_stats[name]["pending"] += 1

    by_analyst = []
    for stats in sorted(analyst_stats.values(), key=lambda x: x["reviewed"], reverse=True):
        avg_hours = stats["total_hours"] / stats["reviewed"] if stats["reviewed"] > 0 else None
        by_analyst.append({
            "analyst": stats["analyst"],
            "reviewed": stats["reviewed"],
            "pending": stats["pending"],
            "avg_hours": round(avg_hours, 1) if avg_hours is not None else None,
        })

    # Overall metrics
    total_reviewed = len(reviewed)
    total_pending = len(pending)
    escalated = sum(1 for f in findings if f.status == FindingStatus.escalated)
    escalation_rate = escalated / len(findings) if findings else 0.0

    # Average disposition hours overall
    disposition_hours = []
    for f in reviewed:
        if f.reviewed_at and f.created_at:
            disposition_hours.append((f.reviewed_at - f.created_at).total_seconds() / 3600)
    avg_disposition_hours = round(sum(disposition_hours) / len(disposition_hours), 1) if disposition_hours else None

    return {
        "by_analyst": by_analyst,
        "total_reviewed": total_reviewed,
        "total_pending": total_pending,
        "escalated": escalated,
        "escalation_rate": round(escalation_rate, 4),
        "escalation_rate_pct": round(escalation_rate * 100, 1),
        "avg_disposition_hours": avg_disposition_hours,
    }


def _generate_charts(
    finding_dicts: list[dict],
    include: dict,
    *,
    fp_analytics: dict | None = None,
    analyst_perf: dict | None = None,
    inst_threat_summaries: list[dict] | None = None,
) -> dict:
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
    if include.get("fp_rate_bar") and fp_analytics:
        result["fp_rate_bar"] = charts.fp_rate_bar(fp_analytics.get("institution_fp_rates", []))
    if include.get("disposition_pie") and fp_analytics:
        result["disposition_pie"] = charts.disposition_pie(fp_analytics.get("disposition_breakdown", []))
    if include.get("analyst_throughput_bar") and analyst_perf:
        result["analyst_throughput_bar"] = charts.analyst_throughput_bar(analyst_perf.get("by_analyst", []))
    if include.get("threat_category_bar") and inst_threat_summaries:
        # Aggregate categories across all institutions
        cat_counts: dict[str, int] = {}
        for s in inst_threat_summaries:
            for cat in s.get("threat_categories", []):
                cat_counts[cat["category"]] = cat_counts.get(cat["category"], 0) + cat["count"]
        agg_cats = sorted(
            [{"category": k, "count": v} for k, v in cat_counts.items()],
            key=lambda x: x["count"],
            reverse=True,
        )
        result["threat_category_bar"] = charts.threat_category_bar(agg_cats)
    return {k: v for k, v in result.items() if v}


async def render_report_html(
    session: AsyncSession,
    *,
    title: str = "DarkDisco Threat Intelligence Report",
    subtitle: str = "Dark Web Threat Intelligence Report",
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    client_id: str | None = None,
    institution_id: str | None = None,
    severities: list[str] | None = None,
    statuses: list[str] | None = None,
    sections: dict | None = None,
    chart_options: dict | None = None,
    truncate_content: bool = True,
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
        "appendix_full_content": False,
        "fp_analytics": False,
        "pattern_effectiveness": False,
        "institution_threat_summary": False,
        "analyst_performance": False,
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
    finding_dicts = _build_finding_dicts(findings, truncate=truncate_content)

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

    # New analytics sections
    fp_analytics_data = _build_fp_analytics(findings) if sec["fp_analytics"] else None
    pattern_data = (
        await _build_pattern_effectiveness(
            session, date_from=date_from, date_to=date_to, institution_id=institution_id,
        )
        if sec["pattern_effectiveness"] else None
    )
    inst_threat_data = (
        await _build_institution_threat_summaries(findings)
        if sec["institution_threat_summary"] else None
    )
    analyst_perf_data = (
        await _build_analyst_performance(findings)
        if sec["analyst_performance"] else None
    )

    chart_data = (
        _generate_charts(
            finding_dicts, chart_inc,
            fp_analytics=fp_analytics_data,
            analyst_perf=analyst_perf_data,
            inst_threat_summaries=inst_threat_data,
        )
        if sec["charts"] else {}
    )

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
        subtitle=subtitle,
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
        fp_analytics=fp_analytics_data,
        pattern_effectiveness=pattern_data,
        inst_threat_summaries=inst_threat_data,
        analyst_performance=analyst_perf_data,
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
