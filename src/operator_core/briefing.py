"""Cross-surface briefing generator.

Two layers coexist in this module:

1. **V3 formatters** (`briefing_markdown / compact / html`) — derive
   multi-format briefings from a `PortfolioSnapshot + AnalysisResponse`.
   Used by the native daemon / dashboard.
2. **V4 morning briefing** (`collect_context / build_prompt / run_once`) —
   synthesizes a short Claude-authored morning brief from real 24-hour
   portfolio data (git log, deploy events, spend, snapshot summary) and
   posts it to the `#claude-chat` Discord channel.

The V4 path is the one the `morning-briefing` scheduled task calls now.
V3 formatters are kept because the native-daemon tests and `/ops`
dashboard page still consume them.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
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


# ── V4 morning briefing (Claude-synthesized, scheduled) ───────────────────────


def _git_24h(path: Path) -> list[str]:
    """Git --oneline for the last 24h. Returns [] on any error."""
    if not (path / ".git").exists():
        return []
    try:
        cr = subprocess.run(
            [
                "git",
                "-C",
                str(path),
                "log",
                "--since=24.hours.ago",
                "--no-decorate",
                "--format=%h %s",
            ],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return []
    if cr.returncode != 0:
        return []
    return [line for line in (cr.stdout or "").splitlines() if line.strip()]


def _fetch_deploy_events(limit: int = 5) -> list[dict[str, Any]]:
    """Best-effort Supabase read of last N deploy events; [] on any failure."""
    url = os.environ.get("SUPABASE_URL") or os.environ.get(
        "NEXT_PUBLIC_SUPABASE_URL"
    )
    key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_KEY")
        or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )
    if not url or not key:
        return []
    try:
        from urllib.request import Request, urlopen

        endpoint = (
            f"{url.rstrip('/')}/rest/v1/deploy_events"
            f"?select=project,status,created_at,url"
            f"&order=created_at.desc&limit={limit}"
        )
        req = Request(
            endpoint,
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Accept": "application/json",
            },
        )
        with urlopen(req, timeout=1.5) as resp:
            import json as _json

            data = _json.loads(resp.read().decode("utf-8"))
        return data if isinstance(data, list) else []
    except Exception:  # noqa: BLE001 — any error → skip
        return []


def _cost_24h(db_path: Path) -> tuple[float, int]:
    """Return (sum_cost_usd, job_count) for the last 24h. (0, 0) on error."""
    if not db_path.exists():
        return 0.0, 0
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        row = conn.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) AS cost, COUNT(*) AS n "
            "FROM jobs WHERE created_at >= ?",
            (cutoff,),
        ).fetchone()
        conn.close()
    except sqlite3.DatabaseError:
        return 0.0, 0
    return float(row["cost"] or 0), int(row["n"] or 0)


def collect_context(settings) -> dict[str, Any]:
    """Build the structured portfolio snapshot a Claude prompt consumes.

    Best-effort everywhere: every data source degrades to an empty/zero
    value if its source is missing. Never raises.
    """
    now = datetime.now(timezone.utc)
    projects_ctx: list[dict[str, Any]] = []
    for p in getattr(settings, "projects", []) or []:
        path = Path(str(p.path))
        commits = _git_24h(path)
        projects_ctx.append({
            "slug": p.slug,
            "commits_24h": commits[:10],
            "commits_24h_count": len(commits),
        })

    cost_usd, job_n = _cost_24h(
        Path(str(getattr(settings, "db_path", "")))
    )
    deploys = _fetch_deploy_events(limit=5)

    return {
        "generated_at": now.isoformat(),
        "projects": projects_ctx,
        "deploys_last_5": deploys,
        "cost_24h_usd": round(cost_usd, 4),
        "jobs_24h": job_n,
    }


def build_prompt(context: dict[str, Any]) -> str:
    """Render the collected context into a short Claude prompt."""
    lines: list[str] = []
    lines.append(
        "You are the operator of a multi-project portfolio. Based on the "
        "last 24 hours of activity below, write a 6-line briefing for the "
        "morning. Line 1 = the single most important thing. Line 6 = the "
        "one move for today. Keep it tight and tactical; no filler."
    )
    lines.append("")
    lines.append(f"Generated: {context.get('generated_at')}")
    lines.append(
        f"Portfolio spend (24h): ${context.get('cost_24h_usd', 0):.2f} "
        f"across {context.get('jobs_24h', 0)} jobs."
    )

    lines.append("")
    lines.append("## Git activity (last 24h)")
    projects = context.get("projects") or []
    if not projects:
        lines.append("- no projects configured.")
    else:
        for p in projects:
            commits = p.get("commits_24h") or []
            if not commits:
                lines.append(f"- {p['slug']}: 0 commits")
                continue
            lines.append(
                f"- {p['slug']}: {p.get('commits_24h_count', len(commits))} commits"
            )
            for c in commits[:5]:
                lines.append(f"    {c}")

    lines.append("")
    lines.append("## Deploy events (last 5)")
    deploys = context.get("deploys_last_5") or []
    if not deploys:
        lines.append("- none / table unreachable.")
    else:
        for d in deploys:
            lines.append(
                f"- {d.get('project', '?')} "
                f"{(d.get('status') or '?').lower()} "
                f"at {d.get('created_at', '?')}"
            )

    lines.append("")
    lines.append(
        "Now produce the 6-line brief. Do not include headers, "
        "bullets, or code fences — just 6 short lines."
    )
    return "\n".join(lines)


SYSTEM_PROMPT = (
    "You are the operator of a portfolio of small software projects. "
    "You write tight, tactical morning briefings for the owner. "
    "Prefer specifics over adjectives. Never invent numbers. "
    "If the data says a project was quiet, say so."
)


def run_once(
    *,
    settings=None,
    notify_fn=None,
    run_agent_fn=None,
    model: str = "claude-sonnet-4-6",
) -> dict[str, Any]:
    """Collect context, call Claude, post to #claude-chat.

    Returns a dict with the prompt, response text, cost, and whether the
    Discord notify succeeded. All dependencies are injectable so tests
    can mock the agent call and the Discord webhook.
    """
    if settings is None:
        try:
            from .settings import load_settings

            settings = load_settings()
        except Exception:
            settings = None

    context = collect_context(settings) if settings is not None else {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "projects": [],
        "deploys_last_5": [],
        "cost_24h_usd": 0.0,
        "jobs_24h": 0,
    }
    prompt = build_prompt(context)

    if run_agent_fn is None:
        from .agent import run_agent as _run_agent

        run_agent_fn = _run_agent

    result = run_agent_fn(
        prompt,
        system=SYSTEM_PROMPT,
        model=model,
        max_turns=1,
    )

    text = getattr(result, "text", "") or ""
    cost = float(getattr(result, "cost_usd", 0) or 0)
    error = getattr(result, "error", None)

    body = text if text else (error or "[no brief produced]")

    if notify_fn is None:
        from .utils.discord import notify as _notify

        notify_fn = _notify

    posted = False
    try:
        posted = bool(
            notify_fn(
                channel="claude_chat",
                title="Morning brief",
                body=body,
                color="blue",
                footer=(
                    f"morning-briefing | Cost: ${cost:.4f} | "
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"
                ),
            )
        )
    except Exception as exc:  # noqa: BLE001
        posted = False
        error = error or f"discord notify failed: {exc}"

    if not error:
        try:
            _write_legacy_briefing_section(
                settings=settings,
                cost=cost,
                projects_in_context=len(context.get("projects") or []),
            )
        except Exception:  # noqa: BLE001
            pass

    return {
        "prompt": prompt,
        "text": text,
        "cost_usd": cost,
        "error": error,
        "posted": posted,
        "projects_in_context": len(context.get("projects") or []),
    }


def _write_legacy_briefing_section(*, settings, cost: float, projects_in_context: int) -> None:
    """Mirror legacy `morning-briefing.py` write so V3 watchdog stays green.

    The V3 watchdog reads `<projects_root>/.operator-status.json` and pages
    when the `briefing` section's timestamp goes stale. Operator-core's
    in-process briefing replaced the legacy script but didn't inherit the
    status-write — leaving the section permanently stale.
    """
    import json

    if settings is None or getattr(settings, "projects_root", None) is None:
        return
    target = Path(settings.projects_root) / ".operator-status.json"
    try:
        existing = json.loads(target.read_text(encoding="utf-8")) if target.exists() else {}
    except (json.JSONDecodeError, OSError):
        existing = {}
    existing["briefing"] = {
        "status": "ok",
        "active_projects": projects_in_context,
        "cost": round(cost, 4),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    target.write_text(json.dumps(existing, indent=2), encoding="utf-8")
