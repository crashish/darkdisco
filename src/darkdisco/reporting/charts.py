"""Chart generation for DarkDisco reports using matplotlib."""

from __future__ import annotations

import base64
import io
from collections import Counter
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# Brand palette
_SEVERITY_COLORS = {
    "critical": "#dc2626",
    "high": "#ea580c",
    "medium": "#eab308",
    "low": "#2563eb",
    "info": "#6b7280",
}

_STATUS_COLORS = {
    "new": "#3b82f6",
    "reviewing": "#f59e0b",
    "escalated": "#ef4444",
    "resolved": "#22c55e",
    "confirmed": "#8b5cf6",
    "dismissed": "#9ca3af",
    "false_positive": "#d1d5db",
}

_SOURCE_COLORS = [
    "#3b82f6", "#ef4444", "#22c55e", "#f59e0b", "#8b5cf6",
    "#ec4899", "#14b8a6", "#f97316", "#06b6d4", "#6366f1",
]


def _fig_to_base64(fig: plt.Figure) -> str:
    """Render a matplotlib figure to a base64 PNG string."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def severity_pie(findings: list[dict]) -> str:
    """Pie chart of findings by severity. Returns base64 PNG."""
    counts = Counter(f.get("severity", "info") for f in findings)
    if not counts:
        return ""

    labels = list(counts.keys())
    sizes = list(counts.values())
    colors = [_SEVERITY_COLORS.get(s, "#6b7280") for s in labels]

    fig, ax = plt.subplots(figsize=(5, 4))
    wedges, _, autotexts = ax.pie(
        sizes, labels=None, colors=colors, autopct="%1.0f%%",
        startangle=90, pctdistance=0.8,
    )
    for t in autotexts:
        t.set_fontsize(9)
    ax.legend(
        wedges, [f"{l.title()} ({c})" for l, c in zip(labels, sizes)],
        loc="center left", bbox_to_anchor=(1, 0.5), fontsize=9,
    )
    ax.set_title("Findings by Severity", fontsize=12, fontweight="bold")
    return _fig_to_base64(fig)


def status_pie(findings: list[dict]) -> str:
    """Pie chart of findings by status. Returns base64 PNG."""
    counts = Counter(f.get("status", "new") for f in findings)
    if not counts:
        return ""

    labels = list(counts.keys())
    sizes = list(counts.values())
    colors = [_STATUS_COLORS.get(s, "#6b7280") for s in labels]

    fig, ax = plt.subplots(figsize=(5, 4))
    wedges, _, autotexts = ax.pie(
        sizes, labels=None, colors=colors, autopct="%1.0f%%",
        startangle=90, pctdistance=0.8,
    )
    for t in autotexts:
        t.set_fontsize(9)
    ax.legend(
        wedges, [f"{l.replace('_', ' ').title()} ({c})" for l, c in zip(labels, sizes)],
        loc="center left", bbox_to_anchor=(1, 0.5), fontsize=9,
    )
    ax.set_title("Findings by Status", fontsize=12, fontweight="bold")
    return _fig_to_base64(fig)


def trend_line(findings: list[dict], date_field: str = "discovered_at") -> str:
    """Line chart showing findings over time. Returns base64 PNG."""
    dates = []
    for f in findings:
        val = f.get(date_field)
        if isinstance(val, str):
            try:
                val = datetime.fromisoformat(val)
            except (ValueError, TypeError):
                continue
        if isinstance(val, datetime):
            dates.append(val.date())
    if not dates:
        return ""

    day_counts: Counter = Counter(dates)
    sorted_days = sorted(day_counts.keys())
    counts = [day_counts[d] for d in sorted_days]

    fig, ax = plt.subplots(figsize=(8, 3.5))
    ax.plot(sorted_days, counts, marker="o", markersize=4, linewidth=2, color="#3b82f6")
    ax.fill_between(sorted_days, counts, alpha=0.1, color="#3b82f6")
    ax.set_title("Findings Over Time", fontsize=12, fontweight="bold")
    ax.set_ylabel("Count")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    fig.autofmt_xdate(rotation=30)
    ax.grid(axis="y", alpha=0.3)
    return _fig_to_base64(fig)


def source_bar(findings: list[dict]) -> str:
    """Bar chart of findings by source type. Returns base64 PNG."""
    counts = Counter(f.get("source_type", "unknown") for f in findings)
    if not counts:
        return ""

    sorted_items = counts.most_common()
    labels = [item[0].replace("_", " ").title() for item in sorted_items]
    sizes = [item[1] for item in sorted_items]
    colors = [_SOURCE_COLORS[i % len(_SOURCE_COLORS)] for i in range(len(labels))]

    fig, ax = plt.subplots(figsize=(8, max(3, len(labels) * 0.4)))
    bars = ax.barh(labels, sizes, color=colors)
    ax.bar_label(bars, padding=3, fontsize=9)
    ax.set_title("Findings by Source Type", fontsize=12, fontweight="bold")
    ax.set_xlabel("Count")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3)
    return _fig_to_base64(fig)


def institution_bar(findings: list[dict]) -> str:
    """Bar chart of findings by institution. Returns base64 PNG."""
    counts = Counter(f.get("institution_name", "Unknown") for f in findings)
    if not counts:
        return ""

    sorted_items = counts.most_common(15)  # Top 15
    labels = [item[0] for item in sorted_items]
    sizes = [item[1] for item in sorted_items]
    colors = [_SOURCE_COLORS[i % len(_SOURCE_COLORS)] for i in range(len(labels))]

    fig, ax = plt.subplots(figsize=(8, max(3, len(labels) * 0.4)))
    bars = ax.barh(labels, sizes, color=colors)
    ax.bar_label(bars, padding=3, fontsize=9)
    ax.set_title("Findings by Institution (Top 15)", fontsize=12, fontweight="bold")
    ax.set_xlabel("Count")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3)
    return _fig_to_base64(fig)


def severity_trend(findings: list[dict], date_field: str = "discovered_at") -> str:
    """Stacked area chart showing severity distribution over time. Returns base64 PNG."""
    from collections import defaultdict

    daily: dict[str, Counter] = defaultdict(Counter)
    for f in findings:
        val = f.get(date_field)
        if isinstance(val, str):
            try:
                val = datetime.fromisoformat(val)
            except (ValueError, TypeError):
                continue
        if isinstance(val, datetime):
            day = val.date()
            sev = f.get("severity", "info")
            daily[str(day)][sev] += 1

    if not daily:
        return ""

    sorted_days = sorted(daily.keys())
    severities = ["critical", "high", "medium", "low", "info"]
    data = {s: [daily[d].get(s, 0) for d in sorted_days] for s in severities}

    fig, ax = plt.subplots(figsize=(8, 3.5))
    ax.stackplot(
        sorted_days,
        *[data[s] for s in severities],
        labels=[s.title() for s in severities],
        colors=[_SEVERITY_COLORS[s] for s in severities],
        alpha=0.8,
    )
    ax.set_title("Severity Trend", fontsize=12, fontweight="bold")
    ax.set_ylabel("Count")
    ax.legend(loc="upper left", fontsize=8)
    if len(sorted_days) > 10:
        ax.set_xticks(sorted_days[::max(1, len(sorted_days) // 8)])
    fig.autofmt_xdate(rotation=30)
    ax.grid(axis="y", alpha=0.3)
    return _fig_to_base64(fig)
