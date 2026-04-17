"""Portfolio state engine — cross-project observation and prioritization.

Collects signals from all project adapters and builds a unified
``PortfolioSnapshot`` that the analysis engine and native surfaces consume.
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .adapters import (
    ADAPTER_REGISTRY,
    ProjectAdapter,
    Urgency,
    list_adapters,
    load_adapters,
)
from .paths import DATA_DIR, STATUS_PATH


PORTFOLIO_STATE_PATH = DATA_DIR / "portfolio-state.json"


# ── Data models ────────────────────────────────────────────────────────────────

@dataclass
class NextAction:
    action: str
    description: str
    urgency: str = "medium"
    estimated_minutes: int = 30
    autonomous_ok: bool = False
    requires_human: bool = True
    recipe: str | None = None
    command: str | None = None


@dataclass
class ProjectState:
    slug: str
    project_type: str
    updated_at: str = ""

    health: str = "unknown"              # green, yellow, red, unknown
    health_details: dict[str, str] = field(default_factory=dict)

    urgency: str = "none"
    urgency_reason: str | None = None
    opportunity: str | None = None
    revenue_proximity: str = "none"

    risk_level: str = "low"
    risk_factors: list[str] = field(default_factory=list)
    trust_level: str = "untested"

    blockers: list[str] = field(default_factory=list)
    human_required: list[str] = field(default_factory=list)
    waiting_on_external: list[str] = field(default_factory=list)

    next_actions: list[NextAction] = field(default_factory=list)
    sprint_candidates: list[str] = field(default_factory=list)
    runnable_workflows: list[str] = field(default_factory=list)

    git_dirty: bool = False
    commits_ahead: int = 0
    open_prs: int = 0
    last_commit_age_hours: float = 0.0

    cost_today_usd: float = 0.0
    cost_this_week_usd: float = 0.0


@dataclass
class PortfolioSnapshot:
    generated_at: str = ""
    projects: dict[str, ProjectState] = field(default_factory=dict)

    top_priority: str = ""
    top_priority_reason: str = ""
    best_use_of_time: list[str] = field(default_factory=list)
    best_agent_work: list[str] = field(default_factory=list)
    blocked_on_human: list[str] = field(default_factory=list)
    critical_issues: list[str] = field(default_factory=list)
    revenue_closest: str = ""

    total_cost_today_usd: float = 0.0
    total_cost_this_week_usd: float = 0.0
    jobs_completed_today: int = 0
    jobs_failed_today: int = 0


# ── Signal collectors ──────────────────────────────────────────────────────────

def _git_state(path: Path) -> dict[str, Any]:
    """Collect git state for a project path."""
    try:
        result = subprocess.run(
            ["git", "status", "--short", "--branch"],
            cwd=str(path),
            capture_output=True,
            text=True,
            timeout=15,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode != 0:
            return {"dirty": True, "branch": "unknown", "ahead": 0}

        lines = result.stdout.splitlines()
        branch_line = lines[0] if lines else "## unknown"
        dirty = any(line and not line.startswith("##") for line in lines)

        ahead = 0
        if "[ahead " in branch_line:
            try:
                ahead = int(branch_line.split("ahead ")[1].split("]")[0].split(",")[0])
            except (IndexError, ValueError):
                pass

        return {"dirty": dirty, "branch": branch_line.removeprefix("## ").strip(), "ahead": ahead}
    except (subprocess.TimeoutExpired, OSError):
        return {"dirty": True, "branch": "unknown", "ahead": 0}


def _last_commit_age_hours(path: Path) -> float:
    """Get hours since last commit."""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%ct"],
            cwd=str(path),
            capture_output=True,
            text=True,
            timeout=10,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode == 0 and result.stdout.strip():
            ts = int(result.stdout.strip())
            return (time.time() - ts) / 3600.0
    except (subprocess.TimeoutExpired, OSError, ValueError):
        pass
    return 9999.0


def _deploy_health(url: str | None) -> str:
    """Quick HTTP health check. Returns 'healthy', 'unhealthy', or 'unknown'."""
    if not url:
        return "unknown"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "operator-daemon/1"})
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
            return "healthy" if 200 <= resp.status < 300 else "unhealthy"
    except Exception:
        return "unhealthy"


# ── Core collection ────────────────────────────────────────────────────────────

def collect_project_state(adapter: ProjectAdapter) -> ProjectState:
    """Collect the current state of a single project from its adapter."""
    now = datetime.now(timezone.utc).isoformat()
    state = ProjectState(
        slug=adapter.slug,
        project_type=adapter.project_type.value,
        updated_at=now,
        revenue_proximity=adapter.revenue_proximity,
        blockers=list(adapter.blockers),
    )

    # Git state
    if adapter.path.exists():
        git = _git_state(adapter.path)
        state.git_dirty = git.get("dirty", False)
        state.commits_ahead = git.get("ahead", 0)
        state.last_commit_age_hours = _last_commit_age_hours(adapter.path)
    else:
        state.health = "red"
        state.health_details["path"] = "directory not found"
        state.risk_factors.append("project directory missing")
        return state

    # Deploy health
    deploy_url = None
    for hc in adapter.health_checks:
        if hc.name == "deploy" and hc.url:
            deploy_url = hc.url
            break
    deploy_status = _deploy_health(deploy_url)
    state.health_details["deploy"] = deploy_status

    # Compute health
    if deploy_status == "unhealthy":
        state.health = "red"
        state.urgency = "high"
        state.urgency_reason = "deploy is unhealthy"
    elif state.git_dirty:
        state.health = "yellow"
    else:
        state.health = "green"

    # Runnable workflows
    state.runnable_workflows = [w.name for w in adapter.workflows if w.autonomous_ok]

    # Risk
    if state.git_dirty:
        state.risk_factors.append("uncommitted changes")
    if state.commits_ahead > 5:
        state.risk_factors.append(f"{state.commits_ahead} commits ahead of remote")
    if adapter.blockers:
        state.risk_factors.extend(adapter.blockers)
    state.risk_level = "high" if len(state.risk_factors) >= 3 else (
        "medium" if state.risk_factors else "low"
    )

    # Trust
    if deploy_status == "healthy" and not state.git_dirty and not adapter.blockers:
        state.trust_level = "high"
    elif deploy_status == "healthy":
        state.trust_level = "medium"
    else:
        state.trust_level = "low"

    return state


def collect_portfolio(adapters: list[ProjectAdapter] | None = None) -> PortfolioSnapshot:
    """Collect state across all projects and build a PortfolioSnapshot."""
    if adapters is None:
        adapters = list_adapters()
    if not adapters:
        load_adapters()
        adapters = list_adapters()

    now = datetime.now(timezone.utc).isoformat()
    snapshot = PortfolioSnapshot(generated_at=now)

    for adapter in adapters:
        state = collect_project_state(adapter)
        snapshot.projects[adapter.slug] = state

    # Cross-project analysis
    _compute_priorities(snapshot)

    return snapshot


def _compute_priorities(snap: PortfolioSnapshot) -> None:
    """Compute cross-project priority rankings."""
    # Urgency ranking
    urgency_order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "none": 4}
    proximity_order = {"live": 0, "near": 1, "far": 2, "none": 3}

    sorted_by_urgency = sorted(
        snap.projects.values(),
        key=lambda s: (urgency_order.get(s.urgency, 4), proximity_order.get(s.revenue_proximity, 3)),
    )

    if sorted_by_urgency:
        top = sorted_by_urgency[0]
        snap.top_priority = top.slug
        snap.top_priority_reason = top.urgency_reason or f"highest urgency ({top.urgency})"

    # Best use of time: critical/high urgency + near revenue
    snap.best_use_of_time = [
        s.slug for s in sorted_by_urgency
        if s.urgency in ("critical", "high") or s.revenue_proximity in ("live", "near")
    ][:5]

    # Best agent work: projects with runnable autonomous workflows
    snap.best_agent_work = [
        s.slug for s in snap.projects.values()
        if s.runnable_workflows and s.health != "red"
    ][:5]

    # Blocked on human
    snap.blocked_on_human = [
        f"{s.slug}: {', '.join(s.blockers)}"
        for s in snap.projects.values()
        if s.blockers
    ]

    # Critical issues
    snap.critical_issues = [
        f"{s.slug}: {s.urgency_reason}"
        for s in snap.projects.values()
        if s.urgency == "critical" and s.urgency_reason
    ]

    # Revenue closest
    for prox in ("live", "near", "far"):
        candidates = [s for s in snap.projects.values() if s.revenue_proximity == prox]
        if candidates:
            snap.revenue_closest = candidates[0].slug
            break


# ── Persistence ────────────────────────────────────────────────────────────────

def _next_action_to_dict(na: NextAction) -> dict[str, Any]:
    return asdict(na)


def _project_state_to_dict(ps: ProjectState) -> dict[str, Any]:
    d = asdict(ps)
    d["next_actions"] = [_next_action_to_dict(na) for na in ps.next_actions]
    return d


def snapshot_to_dict(snap: PortfolioSnapshot) -> dict[str, Any]:
    """Convert snapshot to a JSON-serializable dict."""
    return {
        "generated_at": snap.generated_at,
        "top_priority": snap.top_priority,
        "top_priority_reason": snap.top_priority_reason,
        "best_use_of_time": snap.best_use_of_time,
        "best_agent_work": snap.best_agent_work,
        "blocked_on_human": snap.blocked_on_human,
        "critical_issues": snap.critical_issues,
        "revenue_closest": snap.revenue_closest,
        "total_cost_today_usd": snap.total_cost_today_usd,
        "total_cost_this_week_usd": snap.total_cost_this_week_usd,
        "jobs_completed_today": snap.jobs_completed_today,
        "jobs_failed_today": snap.jobs_failed_today,
        "projects": {
            slug: _project_state_to_dict(ps)
            for slug, ps in snap.projects.items()
        },
    }


def save_snapshot(snap: PortfolioSnapshot, path: Path = PORTFOLIO_STATE_PATH) -> Path:
    """Save the portfolio snapshot to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(snapshot_to_dict(snap), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def load_snapshot(path: Path = PORTFOLIO_STATE_PATH) -> PortfolioSnapshot | None:
    """Load the last saved portfolio snapshot, or None."""
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        snap = PortfolioSnapshot(
            generated_at=data.get("generated_at", ""),
            top_priority=data.get("top_priority", ""),
            top_priority_reason=data.get("top_priority_reason", ""),
            best_use_of_time=data.get("best_use_of_time", []),
            best_agent_work=data.get("best_agent_work", []),
            blocked_on_human=data.get("blocked_on_human", []),
            critical_issues=data.get("critical_issues", []),
            revenue_closest=data.get("revenue_closest", ""),
            total_cost_today_usd=data.get("total_cost_today_usd", 0.0),
            total_cost_this_week_usd=data.get("total_cost_this_week_usd", 0.0),
        )
        for slug, ps_data in data.get("projects", {}).items():
            snap.projects[slug] = ProjectState(
                slug=slug,
                project_type=ps_data.get("project_type", "internal"),
                updated_at=ps_data.get("updated_at", ""),
                health=ps_data.get("health", "unknown"),
                health_details=ps_data.get("health_details", {}),
                urgency=ps_data.get("urgency", "none"),
                urgency_reason=ps_data.get("urgency_reason"),
                revenue_proximity=ps_data.get("revenue_proximity", "none"),
                risk_level=ps_data.get("risk_level", "low"),
                risk_factors=ps_data.get("risk_factors", []),
                trust_level=ps_data.get("trust_level", "untested"),
                blockers=ps_data.get("blockers", []),
                git_dirty=ps_data.get("git_dirty", False),
                commits_ahead=ps_data.get("commits_ahead", 0),
                last_commit_age_hours=ps_data.get("last_commit_age_hours", 0.0),
                runnable_workflows=ps_data.get("runnable_workflows", []),
            )
        return snap
    except (json.JSONDecodeError, OSError, KeyError):
        return None
