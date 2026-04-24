"""
Revenue Cockpit — daily ranked-actions report across the whole portfolio.

Replaces the flat morning-briefing's "every project gets equal weight"
behavior with an explicitly ranked "do these 5 things first" list per
project, plus a top-of-report "today's revenue actions in priority order"
band that pulls from all projects.

Entry points:
    `operator revenue`                  — print to stdout
    `operator revenue --post-discord`   — also post to #claude-chat

Architecture:
    collect_*()     pull raw state from one source (STATUS.md, gh PRs,
                    git working trees, ao_leads, deploy_health, waitlists)
    score_action()  weights an action by revenue-proximity + freshness
    rank_actions()  per-project sort + cross-project top band
    render_*()      stdout / Discord embed
    main()          orchestrates the above for the CLI

Design notes (full spec in SPRINT_REVENUE_COCKPIT_2026-04-23.md):
- Actions are typed (reply, send, fix, deploy, follow_up, ship, decide)
  so the renderer can group + ICON them.
- revenue_proximity is 0..100. 100 = "money in next 7 days if done today";
  0 = "doesn't directly move money" (still useful, just ranked lower).
- freshness is a multiplier 0..1; stale signals lose weight.
- Each collector returns a list[Action]; the orchestrator concatenates,
  scores, ranks, slices.

Data dependencies:
- Per-project STATUS.md scattered across `Projects/` subdirs
- ao_leads (Supabase) — outreach state
- gh CLI — PR state
- git CLI — working-tree state
- operator-core's own snapshot — deploy_health
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Literal


# ── Types ─────────────────────────────────────────────────────────────────────

ActionType = Literal[
    "reply",       # follow up on a real human reply
    "send",        # outbound — a campaign waiting to fire
    "fix",         # bug / blocker holding up revenue
    "deploy",      # ship something that's already built
    "ship",        # finish a near-done feature
    "follow_up",   # touch a warm lead / demo / waitlist signup
    "decide",      # blocked on a Kruz call
    "ops",         # internal hygiene (cleanup, audit)
]


@dataclass
class Action:
    """One unit of work proposed for today."""
    project: str          # "deal-brain", "ai-ops-consulting", "pl-engine", etc.
    type: ActionType
    title: str            # one-line imperative ("Reply to Wayne Wright")
    detail: str = ""      # optional one-paragraph context
    revenue_proximity: int = 0   # 0..100 — how directly this moves money
    freshness: float = 1.0       # 0..1 multiplier — stale = lower
    blocked_on: str = ""         # "Stripe key", "Cory confirm", etc. (empty = unblocked)
    href: str = ""               # link to PR / Supabase row / file
    score: float = field(init=False, default=0.0)

    def compute_score(self) -> None:
        """Weighted score used for ranking. Higher = do first."""
        # revenue_proximity dominates; freshness as multiplier; blocked_on penalty
        base = self.revenue_proximity * self.freshness
        if self.blocked_on:
            base *= 0.4   # blocked actions still surface but not at top
        self.score = round(base, 2)


@dataclass
class ProjectSnapshot:
    """Everything we know about one project right now."""
    slug: str
    actions: list[Action] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    blockers: list[str] = field(default_factory=list)


@dataclass
class CockpitReport:
    generated_at: str
    top_actions: list[Action]                    # cross-project ranked, top 10
    by_project: dict[str, ProjectSnapshot]       # slug -> snapshot
    portfolio_metrics: dict[str, Any]            # aggregates


# ── Collectors ────────────────────────────────────────────────────────────────

def collect_status_md(project_root: Path) -> list[Action]:
    """Parse the project's STATUS.md (if present) for current sprint items.

    Convention: lines beginning with "- [ ]" are open TODOs; "**Blocker:**"
    or "**Blocked on:**" callouts denote blocked items. We treat all open
    items as low-to-medium revenue proximity (10-40) unless they're tagged
    with `[REVENUE]`, `[SHIP]`, or `[REPLY]`.
    """
    status_path = project_root / "STATUS.md"
    if not status_path.exists():
        return []

    actions: list[Action] = []
    project_slug = project_root.name
    try:
        text = status_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    for line in text.splitlines():
        s = line.strip()
        # Open todos: "- [ ]" or legacy "- TODO:" / "* TODO:"
        m = re.match(r"^[-*]\s*\[\s*\]\s*(.*)$", s) or re.match(r"^[-*]\s*TODO:\s*(.*)$", s, re.IGNORECASE)
        if not m:
            continue
        title = m.group(1).strip()
        if not title:
            continue

        # Tags drive revenue_proximity hints
        upper = title.upper()
        rp = 25
        atype: ActionType = "ship"
        if "[REVENUE]" in upper or "[REPLY]" in upper:
            rp = 80
            atype = "reply" if "[REPLY]" in upper else "ship"
        elif "[SHIP]" in upper:
            rp = 60
            atype = "ship"
        elif "[FIX]" in upper:
            rp = 50
            atype = "fix"
        elif "[BLOCKER]" in upper or "BLOCKED" in upper:
            rp = 30
            atype = "fix"

        actions.append(Action(
            project=project_slug,
            type=atype,
            title=title,
            detail="",
            revenue_proximity=rp,
            freshness=1.0,
            blocked_on="",
            href=str(status_path.relative_to(status_path.parent.parent)),
        ))
    return actions


def collect_git_state(project_root: Path) -> tuple[list[Action], dict[str, Any]]:
    """Detect dirty trees, unpushed branches, branches behind origin.

    Returns (actions, metrics). Metrics get folded into ProjectSnapshot.metrics.
    """
    if not (project_root / ".git").exists():
        return [], {}
    actions: list[Action] = []
    metrics: dict[str, Any] = {}
    slug = project_root.name

    def _git(*args: str, timeout: int = 5) -> str:
        try:
            r = subprocess.run(
                ["git", "-C", str(project_root), *args],
                capture_output=True, text=True, timeout=timeout, check=False,
            )
            return r.stdout.strip()
        except Exception:
            return ""

    # Dirty working tree
    porcelain = _git("status", "--porcelain")
    if porcelain:
        n = len(porcelain.splitlines())
        metrics["dirty_files"] = n
        actions.append(Action(
            project=slug, type="ops",
            title=f"Commit or stash {n} dirty file(s)",
            detail="git working tree has uncommitted changes",
            revenue_proximity=15, freshness=1.0,
        ))

    # Unpushed commits
    branch = _git("rev-parse", "--abbrev-ref", "HEAD")
    if branch and branch != "HEAD":
        ahead = _git("rev-list", "--count", f"origin/{branch}..HEAD")
        try:
            ahead_n = int(ahead) if ahead else 0
        except ValueError:
            ahead_n = 0
        if ahead_n > 0:
            metrics["unpushed_commits"] = ahead_n
            actions.append(Action(
                project=slug, type="deploy",
                title=f"Push {ahead_n} unpushed commit(s) on {branch}",
                detail=f"git push origin {branch}",
                revenue_proximity=30, freshness=1.0,
            ))

    return actions, metrics


def collect_open_prs(project_root: Path) -> list[Action]:
    """List open PRs via gh CLI. Stale PRs (>3d no commit) get higher
    revenue_proximity since they're literally finished work waiting to ship.
    """
    if not (project_root / ".git").exists():
        return []
    slug = project_root.name
    try:
        r = subprocess.run(
            ["gh", "pr", "list", "--state", "open", "--json",
             "number,title,updatedAt,url,isDraft"],
            cwd=str(project_root), capture_output=True, text=True,
            timeout=10, check=False,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return []
        prs = json.loads(r.stdout)
    except Exception:
        return []

    actions: list[Action] = []
    now = datetime.now(timezone.utc)
    for pr in prs:
        if pr.get("isDraft"):
            continue
        title = pr.get("title", "")
        url = pr.get("url", "")
        num = pr.get("number")
        updated = pr.get("updatedAt", "")
        try:
            ts = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            stale_days = (now - ts).days
        except Exception:
            stale_days = 0
        rp = 40 if stale_days >= 3 else 25
        actions.append(Action(
            project=slug, type="ship",
            title=f"PR #{num}: {title}",
            detail=f"Open {stale_days}d, last activity {updated[:10]}",
            revenue_proximity=rp, freshness=max(0.3, 1.0 - stale_days * 0.05),
            href=url,
        ))
    return actions


def collect_ao_outreach(supabase) -> tuple[list[Action], dict[str, Any]]:
    """AO outreach state: replies waiting, validated-but-unpitched, today's
    pending touches. Replies in 24h are highest revenue_proximity items in
    the whole report."""
    actions: list[Action] = []
    metrics: dict[str, Any] = {}

    # Replies waiting
    try:
        replies = supabase.table("ao_leads").select(
            "id,business_name,email,city,last_sent_at,replied_at,industry"
        ).eq("status", "replied").order("created_at", desc=True).limit(10).execute().data or []
        metrics["replies_total"] = len(replies)
        for r in replies:
            actions.append(Action(
                project="ai-ops-consulting", type="reply",
                title=f"Reply to {r.get('business_name')} ({r.get('email')})",
                detail=f"{r.get('industry') or ''} in {r.get('city') or ''}",
                revenue_proximity=95, freshness=1.0,
                href=f"https://supabase.com/dashboard/project/ytvtaorgityczrdhhzqv/editor/30201?filter=id:eq:{r.get('id')}",
            ))
    except Exception:
        pass

    # Validated but unpitched (action: top up the personalization batch)
    try:
        unp = supabase.table("ao_leads").select("id,business_name,source").is_(
            "pitch_json", "null"
        ).in_("validation_status", ["valid", "catchall"]).like("source", "ao_trades_%").execute().data or []
        if unp:
            metrics["validated_unpitched"] = len(unp)
            actions.append(Action(
                project="ai-ops-consulting", type="ops",
                title=f"Personalize {len(unp)} validated trades leads (~${len(unp)*0.02:.2f})",
                detail="cd ai-ops-consulting && py outreach/main.py personalize --limit=200",
                revenue_proximity=45, freshness=1.0,
            ))
    except Exception:
        pass

    return actions, metrics


def collect_waitlists(supabase) -> tuple[list[Action], dict[str, Any]]:
    """Quick waitlist counts per product. Big waitlist + no recent contact
    = follow_up action."""
    actions: list[Action] = []
    metrics: dict[str, Any] = {}
    waitlist_tables = {
        "db_waitlist": ("deal-brain", "DealBrain"),
        "vr_waitlist": ("ai-voice-receptionist", "AI Voice Receptionist"),
        "ao_waitlist": ("ai-ops-consulting", "AI Ops"),
        "pp_waitlist": ("prospector-pro", "Prospector Pro"),
    }
    for table, (project, label) in waitlist_tables.items():
        try:
            r = supabase.table(table).select("id", count="exact").limit(1).execute()
            n = getattr(r, "count", None) or 0
            metrics[table] = n
            if n > 0:
                actions.append(Action(
                    project=project, type="follow_up",
                    title=f"{label}: {n} on waitlist — pick 3 to email",
                    detail=f"Top of funnel for {label}; cold start activation",
                    revenue_proximity=55, freshness=1.0,
                ))
        except Exception:
            pass
    return actions, metrics


# ── Orchestration ─────────────────────────────────────────────────────────────

def collect_all(projects_root: Path, supabase=None) -> CockpitReport:
    """Run every collector across every project. Returns a fully-scored report."""
    by_project: dict[str, ProjectSnapshot] = {}

    # Per-project file/git collectors
    for child in sorted(projects_root.iterdir()):
        if not child.is_dir() or child.name.startswith(".") or child.name in ("_archive", "node_modules", "worktrees", "project-docs"):
            continue
        snap = ProjectSnapshot(slug=child.name)
        snap.actions.extend(collect_status_md(child))
        git_actions, git_metrics = collect_git_state(child)
        snap.actions.extend(git_actions)
        snap.metrics.update(git_metrics)
        snap.actions.extend(collect_open_prs(child))
        if snap.actions or snap.metrics:
            by_project[child.name] = snap

    # Cross-project collectors
    if supabase is not None:
        ao_actions, ao_metrics = collect_ao_outreach(supabase)
        ai_ops = by_project.setdefault("ai-ops-consulting", ProjectSnapshot(slug="ai-ops-consulting"))
        ai_ops.actions.extend(ao_actions)
        ai_ops.metrics.update(ao_metrics)
        wl_actions, wl_metrics = collect_waitlists(supabase)
        for a in wl_actions:
            by_project.setdefault(a.project, ProjectSnapshot(slug=a.project)).actions.append(a)
        portfolio_metrics = {**wl_metrics}
    else:
        portfolio_metrics = {}

    # Score every action
    for snap in by_project.values():
        for a in snap.actions:
            a.compute_score()
        snap.actions.sort(key=lambda a: a.score, reverse=True)

    # Cross-project top band
    all_actions: list[Action] = [a for s in by_project.values() for a in s.actions]
    all_actions.sort(key=lambda a: a.score, reverse=True)
    top = all_actions[:10]

    return CockpitReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        top_actions=top,
        by_project=by_project,
        portfolio_metrics=portfolio_metrics,
    )


# ── Renderers ─────────────────────────────────────────────────────────────────

ACTION_GLYPH = {  # for Discord embeds (utf-8 always)
    "reply": "💬",
    "send": "📤",
    "fix": "🔧",
    "deploy": "🚀",
    "ship": "📦",
    "follow_up": "🔔",
    "decide": "❓",
    "ops": "⚙️",
}

ACTION_GLYPH_ASCII = {  # for stdout (Windows cp1252-safe)
    "reply": "[REPLY]",
    "send":  "[SEND] ",
    "fix":   "[FIX]  ",
    "deploy":"[DEPLY]",
    "ship":  "[SHIP] ",
    "follow_up": "[FLUP] ",
    "decide": "[DECID]",
    "ops":   "[OPS]  ",
}


def render_text(report: CockpitReport, top_n: int = 10, per_project: int = 5) -> str:
    """Plain text rendering for stdout / log files. ASCII-only (Windows cp1252 safe)."""
    lines: list[str] = []
    lines.append(f"Revenue Cockpit -- {report.generated_at[:16].replace('T', ' ')} UTC")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"## Today's revenue actions (top {top_n}):")
    if not report.top_actions:
        lines.append("  (no actions found -- collectors may be empty)")
    for i, a in enumerate(report.top_actions[:top_n], 1):
        glyph = ACTION_GLYPH_ASCII.get(a.type, "[?]    ")
        block = f"  BLOCKED: {a.blocked_on}" if a.blocked_on else ""
        lines.append(f"  {i:2}. {glyph} [{a.project:24}] {a.title[:80]} (score {a.score:.0f}){block}")
    lines.append("")
    lines.append("## By project:")
    for slug, snap in sorted(report.by_project.items()):
        if not snap.actions:
            continue
        lines.append(f"\n  {slug}:")
        for a in snap.actions[:per_project]:
            glyph = ACTION_GLYPH_ASCII.get(a.type, "[?]    ")
            lines.append(f"    {glyph} {a.title[:90]} (score {a.score:.0f})")
        if snap.metrics:
            metric_str = ", ".join(f"{k}={v}" for k, v in snap.metrics.items())
            lines.append(f"    metrics: {metric_str}")
    if report.portfolio_metrics:
        lines.append("\n## Portfolio metrics:")
        for k, v in report.portfolio_metrics.items():
            lines.append(f"  {k}: {v}")
    return "\n".join(lines)


def render_discord(report: CockpitReport, top_n: int = 8) -> dict:
    """Discord embed payload. Caller posts via existing notify_with_fallback."""
    fields = []
    top_lines = []
    for i, a in enumerate(report.top_actions[:top_n], 1):
        glyph = ACTION_GLYPH.get(a.type, "•")
        block = " 🚫" if a.blocked_on else ""
        top_lines.append(f"`{i:2}` {glyph} **[{a.project}]** {a.title}{block}")
    fields.append({
        "name": f"🎯 Today's revenue actions (top {top_n})",
        "value": "\n".join(top_lines)[:1024] if top_lines else "_no actions_",
        "inline": False,
    })

    if report.portfolio_metrics:
        metric_lines = [f"**{k}:** {v}" for k, v in report.portfolio_metrics.items()]
        fields.append({
            "name": "📊 Portfolio metrics",
            "value": "\n".join(metric_lines)[:1024],
            "inline": False,
        })

    by_proj_summary = []
    for slug, snap in sorted(report.by_project.items())[:8]:
        n_actions = len(snap.actions)
        n_blocked = sum(1 for a in snap.actions if a.blocked_on)
        by_proj_summary.append(f"`{slug}`: {n_actions} action(s){' · ' + str(n_blocked) + ' blocked' if n_blocked else ''}")
    fields.append({
        "name": "📁 Project rollup",
        "value": "\n".join(by_proj_summary)[:1024] or "_none_",
        "inline": False,
    })

    return {
        "title": "Revenue Cockpit",
        "description": f"Generated {report.generated_at[:16].replace('T', ' ')} UTC",
        "color": 0x6366f1,
        "fields": fields,
        "footer": {"text": f"operator-core revenue · {len(report.by_project)} projects · {sum(len(s.actions) for s in report.by_project.values())} total actions"},
    }


# ── CLI entry ─────────────────────────────────────────────────────────────────

def _supabase_client():
    """Lazy supabase import + creds resolution."""
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_KEY", "").strip() or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not url or not key:
        return None
    try:
        from supabase import create_client
        return create_client(url, key)
    except Exception:
        return None


def main(argv: list[str] | None = None) -> int:
    # Force utf-8 stdout so emoji/ASCII mix doesn't crash on Windows cp1252.
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    args = list(argv if argv is not None else sys.argv[1:])
    post_discord = "--post-discord" in args
    top_n = 10
    for a in args:
        if a.startswith("--top="):
            try:
                top_n = int(a.split("=", 1)[1])
            except Exception:
                pass

    from .paths import PROJECTS_ROOT
    sb = _supabase_client()
    report = collect_all(Path(PROJECTS_ROOT), supabase=sb)
    print(render_text(report, top_n=top_n))

    if post_discord:
        try:
            from .utils.discord import notify
            embed = render_discord(report, top_n=top_n)
            # Build a richer body containing the actual top actions, not
            # just the description (notify() doesn't pass through fields).
            body_lines = [embed["description"], ""]
            for fld in embed.get("fields", []):
                body_lines.append(f"**{fld['name']}**")
                body_lines.append(fld["value"])
                body_lines.append("")
            ok = notify(
                channel="projects",
                title=embed["title"],
                body="\n".join(body_lines)[:3800],
                color="indigo",
                footer=embed["footer"]["text"],
            )
            print(f"\n[discord] post {'OK' if ok else 'FAILED'}")
        except Exception as e:
            print(f"\n[discord] post failed: {e}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
