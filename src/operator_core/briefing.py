"""Cross-surface briefing generator for Operator V3.

Generates briefings in three formats:
  - markdown (for IDE / Claude Code conversations)
  - compact (for Discord messages, under 2000 chars)
  - html (for the /ops dashboard page)

All formats are derived from the same PortfolioSnapshot + AnalysisResponse.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .analysis import AnalysisResponse, analyze_portfolio_local
from .portfolio import PortfolioSnapshot, ProjectState


# ── Markdown (IDE) ─────────────────────────────────────────────────────────────

def briefing_markdown(
    snapshot: PortfolioSnapshot,
    analysis: AnalysisResponse | None = None,
) -> str:
    """Full markdown briefing for IDE display."""
    if analysis is None:
        analysis = analyze_portfolio_local(snapshot)

    lines: list[str] = []
    lines.append(f"# Portfolio Briefing — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")

    # Top priority
    if snapshot.top_priority:
        lines.append(f"**Top priority:** {snapshot.top_priority} — {snapshot.top_priority_reason}")
        lines.append("")

    # Critical issues
    if snapshot.critical_issues:
        lines.append("## Critical Issues")
        for issue in snapshot.critical_issues:
            lines.append(f"- 🔴 {issue}")
        lines.append("")

    # Best use of time
    if snapshot.best_use_of_time:
        lines.append("## Best Use of Time Today")
        for i, slug in enumerate(snapshot.best_use_of_time[:5], 1):
            state = snapshot.projects.get(slug)
            reason = state.urgency_reason if state and state.urgency_reason else state.urgency if state else ""
            lines.append(f"{i}. **{slug}** — {reason}")
        lines.append("")

    # Agent work
    if snapshot.best_agent_work:
        lines.append("## Agent-Ready Tasks")
        for slug in snapshot.best_agent_work[:5]:
            state = snapshot.projects.get(slug)
            workflows = ", ".join(state.runnable_workflows[:3]) if state else ""
            lines.append(f"- {slug}: {workflows}")
        lines.append("")

    # Blocked
    if snapshot.blocked_on_human:
        lines.append("## Blocked on Human")
        for item in snapshot.blocked_on_human:
            lines.append(f"- ⏸️ {item}")
        lines.append("")

    # Project health grid
    lines.append("## Project Health")
    lines.append("| Project | Health | Urgency | Revenue | Trust | Git |")
    lines.append("|---------|--------|---------|---------|-------|-----|")
    for slug, state in sorted(snapshot.projects.items()):
        health_icon = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(state.health, "⚪")
        git = "dirty" if state.git_dirty else "clean"
        lines.append(
            f"| {slug} | {health_icon} {state.health} | {state.urgency} | "
            f"{state.revenue_proximity} | {state.trust_level} | {git} |"
        )
    lines.append("")

    # Analysis summary
    if analysis.summary:
        lines.append("## Analysis")
        lines.append(analysis.summary)
        lines.append("")

    # Findings
    if analysis.findings:
        high = [f for f in analysis.findings if f.severity in ("critical", "high")]
        if high:
            lines.append("## High-Severity Findings")
            for finding in high[:5]:
                lines.append(f"- **[{finding.severity}]** {finding.title}")
                if finding.detail:
                    lines.append(f"  {finding.detail}")
            lines.append("")

    return "\n".join(lines)


# ── Compact (Discord) ─────────────────────────────────────────────────────────

def briefing_compact(
    snapshot: PortfolioSnapshot,
    analysis: AnalysisResponse | None = None,
) -> str:
    """Compact briefing for Discord (under 2000 chars)."""
    if analysis is None:
        analysis = analyze_portfolio_local(snapshot)

    lines: list[str] = []
    lines.append(f"**Portfolio Briefing** ({len(snapshot.projects)} projects)")

    # Top priority
    if snapshot.top_priority:
        lines.append(f"🎯 **Priority:** {snapshot.top_priority} — {snapshot.top_priority_reason}")

    # Health summary
    health_counts = {"green": 0, "yellow": 0, "red": 0, "unknown": 0}
    for state in snapshot.projects.values():
        health_counts[state.health] = health_counts.get(state.health, 0) + 1
    lines.append(
        f"🏥 Health: {health_counts['green']}🟢 {health_counts['yellow']}🟡 "
        f"{health_counts['red']}🔴 {health_counts['unknown']}⚪"
    )

    # Critical
    if snapshot.critical_issues:
        lines.append(f"🚨 Critical: {', '.join(snapshot.critical_issues[:3])}")

    # Best use of time
    if snapshot.best_use_of_time:
        top3 = snapshot.best_use_of_time[:3]
        lines.append(f"📌 Work on: {', '.join(top3)}")

    # Agent work
    if snapshot.best_agent_work:
        lines.append(f"🤖 Agents: {', '.join(snapshot.best_agent_work[:3])}")

    # Blocked
    if snapshot.blocked_on_human:
        lines.append(f"⏸️ Blocked: {len(snapshot.blocked_on_human)} item(s)")

    # Revenue
    if snapshot.revenue_closest:
        lines.append(f"💰 Revenue closest: {snapshot.revenue_closest}")

    return "\n".join(lines)


# ── HTML (Dashboard) ───────────────────────────────────────────────────────────

def briefing_html_section(
    snapshot: PortfolioSnapshot,
    analysis: AnalysisResponse | None = None,
) -> str:
    """HTML section for the /ops dashboard."""
    if analysis is None:
        analysis = analyze_portfolio_local(snapshot)

    parts: list[str] = []
    parts.append('<div class="portfolio-section">')
    parts.append(f'<h2>Portfolio ({len(snapshot.projects)} projects)</h2>')

    # Priority
    if snapshot.top_priority:
        parts.append(f'<p class="priority">🎯 <strong>{snapshot.top_priority}</strong> — {_esc(snapshot.top_priority_reason)}</p>')

    # Health grid
    parts.append('<table><tr><th>Project</th><th>Health</th><th>Urgency</th><th>Revenue</th><th>Git</th></tr>')
    for slug, state in sorted(snapshot.projects.items()):
        health_class = {"green": "ok", "yellow": "warn", "red": "err"}.get(state.health, "")
        git = "dirty" if state.git_dirty else "clean"
        parts.append(
            f'<tr><td>{_esc(slug)}</td><td class="{health_class}">{_esc(state.health)}</td>'
            f'<td>{_esc(state.urgency)}</td><td>{_esc(state.revenue_proximity)}</td>'
            f'<td>{_esc(git)}</td></tr>'
        )
    parts.append('</table>')

    # Critical
    if snapshot.critical_issues:
        parts.append('<div class="critical">')
        for issue in snapshot.critical_issues:
            parts.append(f'<p>🔴 {_esc(issue)}</p>')
        parts.append('</div>')

    # Blocked
    if snapshot.blocked_on_human:
        parts.append(f'<p>⏸️ {len(snapshot.blocked_on_human)} item(s) blocked on human</p>')

    parts.append('</div>')
    return "\n".join(parts)


def _esc(text: str) -> str:
    """Minimal HTML escaping."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── Priorities endpoint ────────────────────────────────────────────────────────

def priorities_json(snapshot: PortfolioSnapshot) -> dict[str, Any]:
    """Build the /priorities JSON response."""
    human_actions: list[dict[str, str]] = []
    agent_actions: list[dict[str, str]] = []

    for slug in snapshot.best_use_of_time[:5]:
        state = snapshot.projects.get(slug)
        if state:
            human_actions.append({
                "project": slug,
                "action": state.urgency_reason or "work on this project",
                "urgency": state.urgency,
            })

    for slug in snapshot.best_agent_work[:5]:
        state = snapshot.projects.get(slug)
        if state and state.runnable_workflows:
            agent_actions.append({
                "project": slug,
                "action": state.runnable_workflows[0],
                "recipe": "auto-select",
            })

    return {
        "for_human": human_actions,
        "for_agents": agent_actions,
        "blocked": snapshot.blocked_on_human,
        "revenue_closest": snapshot.revenue_closest,
    }
