"""portfolio_health -- daily portfolio health monitor dashboard.

Reads cross-portfolio signals (status-spec component files, recent
events-ndjson activity, git history, open GitHub PRs) and renders a
single Palantir-themed dashboard to ``war-room/portfolio-health.html``
and ``war-room/portfolio-health.md`` via the ``templated-dashboards``
library.

Lives as an operator-core Recipe so it dogfoods the recipe framework
and the schedule.yaml installer. No LLM calls, ~$0 cost.

Hard rules:
  - ASCII-only.
  - No filesystem scraping for status -- use status-spec readers.
  - Don't fabricate status; show "unknown / no emitter" instead.
  - Don't touch sibling-agent working files (pool-prospector,
    ai-ops-consulting).
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


# ----------------------------------------------------------------------------
# Tracked-project catalog
#
# Canonical list comes from the Projects/CLAUDE.md router + MEMORY.md
# "Complete Project Directory". Each entry maps a display name to the
# folder under Projects/ and an optional GitHub slug "owner/repo" used for
# the gh CLI lookup. ``status_recipes`` lists the operator recipes whose
# component status implies health for this project (used as a fallback
# when no project-emitted status-spec file exists).
# ----------------------------------------------------------------------------

TRACKED_PROJECTS: list[dict[str, Any]] = [
    # --- priority SaaS -------------------------------------------------------
    {"name": "deal-brain", "dir": "deal-brain", "repo": "kjhholt-alt/deal-brain",
     "category": "SaaS", "status_recipes": ["deploy_checker"]},
    {"name": "prospector-pro", "dir": "prospector-pro", "repo": "kjhholt-alt/prospector-pro",
     "category": "SaaS", "status_recipes": ["outreach_pulse", "deploy_checker"]},
    {"name": "ai-ops-consulting", "dir": "ai-ops-consulting", "repo": "kjhholt-alt/ai-ops-consulting",
     "category": "SaaS", "status_recipes": ["audit_intake", "deploy_checker"]},
    {"name": "ai-voice-receptionist", "dir": "ai-voice-receptionist",
     "repo": "kjhholt-alt/ai-voice-receptionist",
     "category": "SaaS", "status_recipes": ["deploy_checker"]},
    {"name": "pool-prospector", "dir": "pool-prospector", "repo": "kjhholt-alt/pool-prospector",
     "category": "SaaS", "status_recipes": ["deploy_checker"]},
    # --- client projects -----------------------------------------------------
    {"name": "outdoor-crm", "dir": "outdoor-crm", "repo": "kjhholt-alt/outdoor-crm",
     "category": "Client", "status_recipes": ["client_health"]},
    {"name": "website-factory", "dir": "website-factory",
     "repo": "kjhholt-alt/website-factory",
     "category": "Client", "status_recipes": ["client_health"]},
    {"name": "municipal-crm", "dir": "municipal-crm", "repo": "kjhholt-alt/municipal-crm",
     "category": "Client", "status_recipes": ["client_health"], "protected": True},
    # --- infra / tooling -----------------------------------------------------
    {"name": "operator-core", "dir": "operator-core", "repo": "kjhholt-alt/operator-core",
     "category": "Infra", "status_recipes": ["watchdog", "status_rollup"]},
    {"name": "operator-scripts", "dir": "operator-scripts",
     "repo": "kjhholt-alt/operator-scripts",
     "category": "Infra", "status_recipes": []},
    {"name": "pl-engine", "dir": "pl-engine", "repo": "kjhholt-alt/pl-engine",
     "category": "Infra", "status_recipes": ["pl_auditor"]},
    {"name": "pl-engine-dashboard", "dir": "pl-engine-dashboard",
     "repo": "kjhholt-alt/pl-engine-dashboard",
     "category": "Infra", "status_recipes": []},
    {"name": "ag-market-pulse", "dir": "ag-market-pulse",
     "repo": "kjhholt-alt/ag-market-pulse",
     "category": "Infra", "status_recipes": []},
    {"name": "pc-bottleneck-analyzer", "dir": "pc-bottleneck-analyzer",
     "repo": "kjhholt-alt/pc-bottleneck-analyzer",
     "category": "Infra", "status_recipes": []},
    {"name": "close-copilot", "dir": "close-copilot", "repo": "kjhholt-alt/close-copilot",
     "category": "Infra", "status_recipes": []},
    # --- libraries (the new portable layer) ---------------------------------
    {"name": "status-spec", "dir": "status-spec", "repo": "kjhholt-alt/status-spec",
     "category": "Lib", "status_recipes": []},
    {"name": "events-ndjson", "dir": "events-ndjson", "repo": "kjhholt-alt/events-ndjson",
     "category": "Lib", "status_recipes": []},
    {"name": "templated-dashboards", "dir": "templated-dashboards",
     "repo": "kjhholt-alt/templated-dashboards",
     "category": "Lib", "status_recipes": []},
    {"name": "agentbridge", "dir": "agentbridge", "repo": "kjhholt-alt/agentbridge",
     "category": "Lib", "status_recipes": []},
    # --- games ---------------------------------------------------------------
    {"name": "game-forge", "dir": "game-forge", "repo": "kjhholt-alt/game-forge",
     "category": "Game", "status_recipes": []},
    {"name": "quietwoods", "dir": "quiet-woods", "repo": "kjhholt-alt/quiet-woods",
     "category": "Game", "status_recipes": []},
    {"name": "case-zero-game", "dir": "case-zero-game",
     "repo": "kjhholt-alt/case-zero-game",
     "category": "Game", "status_recipes": []},
    {"name": "warcouncil", "dir": "warcouncil", "repo": "kjhholt-alt/warcouncil",
     "category": "Game", "status_recipes": []},
    # --- portfolio / business -----------------------------------------------
    {"name": "war-room", "dir": "war-room", "repo": "kjhholt-alt/war-room",
     "category": "Ops", "status_recipes": []},
    {"name": "portfolio", "dir": "portfolio", "repo": "kjhholt-alt/portfolio",
     "category": "Ops", "status_recipes": []},
]


STALE_THRESHOLD_HOURS = 48
RECENT_EVENT_WINDOW_DAYS = 7


# ----------------------------------------------------------------------------
# Path helpers
# ----------------------------------------------------------------------------

def _projects_root() -> Path:
    override = os.environ.get("OPERATOR_PROJECTS_DIR")
    if override:
        return Path(override)
    home = Path.home()
    candidate = home / "Desktop" / "Projects"
    if candidate.exists():
        return candidate
    return home


def _war_room_dir() -> Path:
    override = os.environ.get("OPERATOR_WAR_ROOM_DIR")
    if override:
        return Path(override)
    return _projects_root() / "war-room"


def _status_dir() -> Path:
    return Path(
        os.environ.get(
            "OPERATOR_STATUS_DIR",
            str(Path.home() / ".operator" / "data" / "status"),
        )
    )


def _events_dir() -> Path:
    return Path(
        os.environ.get(
            "OPERATOR_EVENTS_DIR",
            str(Path.home() / ".operator" / "data"),
        )
    )


# ----------------------------------------------------------------------------
# Status-spec / events-ndjson readers
# ----------------------------------------------------------------------------

def _read_component_status(name: str) -> dict[str, Any] | None:
    """Return the status-spec component dict for ``name`` or None.

    Uses the public status-spec library if available; falls back to direct
    JSON read against OPERATOR_STATUS_DIR. Both shapes share the
    ``status``, ``last_run``, ``cost_usd``, ``error`` keys we read.
    """
    target = _status_dir() / f"{name}.json"
    if not target.exists():
        return None
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _read_project_status_doc(project_slug: str) -> dict[str, Any] | None:
    """Try to load a project-emitted status-spec/v1 document.

    A project that has adopted the status-spec writer will have a file
    at ``~/.operator/data/status/<project>.json`` with shape from
    StatusDocument.to_dict(). If it's missing we return None so the
    caller can fall back / show 'unknown'.
    """
    return _read_component_status(project_slug)


def _read_runs_events(window: timedelta) -> list[dict[str, Any]]:
    """Read recent events from runs.ndjson.

    Tries the published events-ndjson library first; if its strict
    validator rejects legacy events written by the operator-core
    vendored stub (which uses different envelope keys) we fall back
    to a plain JSON line read.
    """
    target = _events_dir() / "runs.ndjson"
    if not target.exists():
        return []
    cutoff = datetime.now(timezone.utc) - window
    events: list[dict[str, Any]] = []
    with open(target, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                continue
            ts = ev.get("ts")
            if not ts:
                continue
            try:
                # both vendored-stub and v1 envelope use ISO 8601
                ev_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
            if ev_dt >= cutoff:
                events.append(ev)
    return events


def _read_cost_events(window: timedelta) -> list[dict[str, Any]]:
    target = _events_dir() / "cost.ndjson"
    if not target.exists():
        return []
    cutoff = datetime.now(timezone.utc) - window
    events: list[dict[str, Any]] = []
    with open(target, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                continue
            ts = ev.get("ts")
            if not ts:
                continue
            try:
                ev_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
            if ev_dt >= cutoff:
                events.append(ev)
    return events


# ----------------------------------------------------------------------------
# Git + gh helpers
# ----------------------------------------------------------------------------

def _run_subprocess(argv: list[str], cwd: Path | None = None,
                    timeout: float = 10.0) -> tuple[int, str, str]:
    try:
        res = subprocess.run(
            argv,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return res.returncode, res.stdout.strip(), res.stderr.strip()
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 124, "", str(exc)


def _git_last_commit(project_dir: Path) -> dict[str, Any] | None:
    if not (project_dir / ".git").exists():
        return None
    rc, out, _err = _run_subprocess(
        ["git", "log", "-1", "--format=%H%x00%cI%x00%s%x00%an"],
        cwd=project_dir,
    )
    if rc != 0 or not out:
        return None
    parts = out.split("\x00")
    if len(parts) < 4:
        return None
    sha, iso_ts, subject, author = parts[0], parts[1], parts[2], parts[3]
    return {
        "sha": sha[:8],
        "ts": iso_ts,
        "subject": subject,
        "author": author,
    }


def _git_branches_with_work(project_dir: Path) -> list[str]:
    if not (project_dir / ".git").exists():
        return []
    rc, out, _err = _run_subprocess(
        ["git", "branch", "--format=%(refname:short)"],
        cwd=project_dir,
    )
    if rc != 0:
        return []
    return [b.strip() for b in out.splitlines() if b.strip() and not b.strip().startswith("(")]


def _gh_open_prs(repo_slug: str) -> list[dict[str, Any]]:
    """Return open PRs for ``repo_slug`` via the gh CLI.

    Returns [] silently on any failure (gh not logged in, repo missing,
    network down). The dashboard surfaces gh-cli health separately.
    """
    rc, out, _err = _run_subprocess(
        [
            "gh", "pr", "list",
            "--repo", repo_slug,
            "--state", "open",
            "--json", "number,title,author,updatedAt,isDraft,url",
            "--limit", "20",
        ],
        timeout=15.0,
    )
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return []


def _gh_logged_in() -> bool:
    rc, _out, _err = _run_subprocess(["gh", "auth", "status"], timeout=10.0)
    return rc == 0


# ----------------------------------------------------------------------------
# Aggregation
# ----------------------------------------------------------------------------

def _hours_since(iso_ts: str | None) -> float | None:
    if not iso_ts:
        return None
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    delta = datetime.now(timezone.utc) - dt
    return delta.total_seconds() / 3600.0


def _classify_health(project: dict[str, Any], status_doc: dict[str, Any] | None,
                     fallback_components: list[dict[str, Any]],
                     last_commit: dict[str, Any] | None) -> tuple[str, str]:
    """Return (tone, reason). Tone is one of good|warn|bad|neutral."""
    # 1. Project emitted a status-spec/v1 document -> trust its health.
    if status_doc and "health" in status_doc:
        h = status_doc.get("health", "")
        if h == "green":
            return ("good", status_doc.get("summary") or "status: green")
        if h == "yellow":
            return ("warn", status_doc.get("summary") or "status: yellow")
        if h == "red":
            return ("bad", status_doc.get("summary") or "status: red")

    # 2. Fallback to associated recipe component statuses.
    bad = [c for c in fallback_components if c.get("status") == "error"]
    warn = [c for c in fallback_components if c.get("status") == "warn"]
    if bad:
        names = ", ".join(c.get("name", "?") for c in bad)
        return ("bad", f"recipes erroring: {names}")
    if warn:
        names = ", ".join(c.get("name", "?") for c in warn)
        return ("warn", f"recipes warning: {names}")

    # 3. No emitter and no associated recipes -> neutral.
    if not status_doc and not fallback_components:
        return ("neutral", "no status emitter")

    # 4. Recipes ok + maybe stale commit?
    if last_commit:
        hrs = _hours_since(last_commit.get("ts"))
        if hrs is not None and hrs > 24 * 14:
            return ("warn", f"no commits in {int(hrs/24)} days")
    return ("good", "ok")


def _aggregate(projects_root: Path, gh_available: bool) -> dict[str, Any]:
    """Build the portfolio-health intermediate state.

    Pure function of disk + subprocess; no Discord, no LLM.
    """
    runs_window = _read_runs_events(timedelta(days=RECENT_EVENT_WINDOW_DAYS))
    cost_window = _read_cost_events(timedelta(days=RECENT_EVENT_WINDOW_DAYS))

    project_rows: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []
    counts = {"good": 0, "warn": 0, "bad": 0, "neutral": 0}
    total_cost = 0.0
    cost_by_project: dict[str, float] = {}

    for project in TRACKED_PROJECTS:
        slug = project["name"]
        project_dir = projects_root / project["dir"]
        exists = project_dir.exists()

        # Project status emitter doesn't require the dir to exist locally --
        # status writes go to ~/.operator/data/status/<slug>.json regardless.
        status_doc = _read_project_status_doc(slug)
        # Pull recipe-keyed component status as fallback signal.
        fallback_components: list[dict[str, Any]] = []
        for recipe_name in project.get("status_recipes", []):
            comp = _read_component_status(recipe_name)
            if comp:
                fallback_components.append(comp)

        last_commit = _git_last_commit(project_dir) if exists else None
        branches = _git_branches_with_work(project_dir) if exists else []
        prs = _gh_open_prs(project["repo"]) if (gh_available and exists) else []

        tone, reason = _classify_health(project, status_doc, fallback_components, last_commit)
        counts[tone] = counts.get(tone, 0) + 1

        # Stale alert: status_doc exists but >48h old.
        if status_doc:
            ts = status_doc.get("ts") or status_doc.get("last_run")
            hrs = _hours_since(ts)
            if hrs is not None and hrs > STALE_THRESHOLD_HOURS:
                stale.append({
                    "project": slug,
                    "hours_stale": round(hrs, 1),
                    "last_update": ts,
                })

        # Cost rollup -- if we ever wire per-project tagging into events, sum here.
        # For now we attribute cost by recipe -> nearest project. Future work.

        project_rows.append({
            "slug": slug,
            "category": project.get("category", ""),
            "exists": exists,
            "protected": bool(project.get("protected", False)),
            "tone": tone,
            "reason": reason,
            "status_doc": status_doc,
            "fallback_components": fallback_components,
            "last_commit": last_commit,
            "branch_count": len(branches),
            "open_prs": prs,
            "repo": project["repo"],
        })

    for ev in cost_window:
        amt = ev.get("amount_usd") or ev.get("payload", {}).get("amount_usd") or 0.0
        try:
            amt = float(amt)
        except (TypeError, ValueError):
            amt = 0.0
        total_cost += amt
        recipe = ev.get("recipe") or "unknown"
        cost_by_project[recipe] = cost_by_project.get(recipe, 0.0) + amt

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "projects_root": str(projects_root),
        "projects": project_rows,
        "counts": counts,
        "stale": stale,
        "runs_window": runs_window,
        "cost_window": cost_window,
        "total_cost_7d": total_cost,
        "cost_by_project": cost_by_project,
        "gh_available": gh_available,
    }


# ----------------------------------------------------------------------------
# Dashboard IR
# ----------------------------------------------------------------------------

def _build_dashboard(state: dict[str, Any]) -> Any:
    """Construct the templated-dashboards Dashboard IR."""
    from dashboards import Dashboard

    counts = state["counts"]
    total = sum(counts.values())
    generated = state["generated_at"]

    db = Dashboard(
        "Portfolio Health",
        subtitle=f"{total} projects tracked - generated {generated}",
        theme="palantir",
    )

    # Top banner KPIs
    db.section("Overview", layout="grid")
    db.kpi("Tracked", total, tone="neutral")
    db.kpi("Green", counts.get("good", 0), tone="good")
    db.kpi("Yellow", counts.get("warn", 0), tone="warn")
    db.kpi("Red", counts.get("bad", 0), tone="bad")
    db.kpi("Unknown", counts.get("neutral", 0), tone="neutral")
    db.kpi(
        "7d cost",
        f"${state['total_cost_7d']:.4f}",
        tone="neutral",
    )

    # Stale alerts (only if any)
    if state["stale"]:
        db.section("Stale alerts", layout="stack")
        rows = []
        for s in state["stale"]:
            rows.append([s["project"], f"{s['hours_stale']}h", s["last_update"] or ""])
        db.table(
            headers=["project", "hours stale", "last update"],
            rows=rows,
            caption=f"projects with no status update in {STALE_THRESHOLD_HOURS}h",
        )
    else:
        db.section("Stale alerts", layout="stack")
        db.callout(
            f"No projects exceed the {STALE_THRESHOLD_HOURS}h staleness threshold.",
            tone="good",
        )

    # Per-project grid
    db.section("Projects", layout="grid")
    for p in state["projects"]:
        # Inline status payload for the StatusCard component. The card
        # renderer expects ``tier`` and ``summary`` keys (status-spec
        # stub shape used by templated-dashboards); we map our tone->tier.
        tier_map = {"good": "good", "warn": "warn", "bad": "bad", "neutral": "neutral"}
        last_event = ""
        if p["last_commit"]:
            last_event = (
                f"git: {p['last_commit']['sha']} - {p['last_commit']['subject'][:60]}"
            )
        elif p["status_doc"]:
            ev = p["status_doc"].get("last_event") or {}
            last_event = ev.get("summary") or ev.get("type") or ""
        db.status_card(
            p["slug"],
            inline_status={
                "project": p["slug"],
                "tier": tier_map.get(p["tone"], "neutral"),
                "summary": f"[{p['category']}] {p['reason']}"
                + ((" - " + last_event) if last_event else ""),
                "last_update": (p["last_commit"] or {}).get("ts"),
            },
        )

    # Recent runs timeline (last 20 events)
    db.section("Recent runs", layout="stack")
    recent = sorted(
        state["runs_window"],
        key=lambda e: e.get("ts", ""),
        reverse=True,
    )[:20]
    if recent:
        events = []
        for ev in recent:
            recipe = ev.get("recipe") or "?"
            kind = ev.get("kind") or ev.get("event_type") or "event"
            status = (
                ev.get("status")
                or (ev.get("payload") or {}).get("status")
                or ""
            )
            tone = "neutral"
            if status == "ok":
                tone = "good"
            elif status in ("error", "timeout", "verify_failed"):
                tone = "bad"
            elif status in ("warn", "budget_exceeded"):
                tone = "warn"
            events.append({
                "when": ev.get("ts", ""),
                "title": f"{recipe} - {kind}",
                "detail": status or None,
                "tone": tone,
            })
        db.timeline(events)
    else:
        db.callout("No recent run events.", tone="neutral")

    # Cost rollup table
    db.section("Cost rollup (7d)", layout="stack")
    if state["cost_by_project"]:
        rows = sorted(
            ((k, v) for k, v in state["cost_by_project"].items()),
            key=lambda kv: kv[1],
            reverse=True,
        )
        db.table(
            headers=["recipe / project", "cost USD"],
            rows=[[k, f"${v:.4f}"] for k, v in rows],
            caption=f"total: ${state['total_cost_7d']:.4f}",
        )
    else:
        db.callout("No cost events in the 7d window.", tone="neutral")

    # Open PRs across all repos
    db.section("Open PRs", layout="stack")
    pr_rows: list[list[str]] = []
    for p in state["projects"]:
        for pr in p.get("open_prs", []):
            pr_rows.append([
                p["slug"],
                f"#{pr.get('number','?')}",
                (pr.get("title") or "")[:80],
                (pr.get("author") or {}).get("login", "?")
                if isinstance(pr.get("author"), dict)
                else str(pr.get("author") or "?"),
                "draft" if pr.get("isDraft") else "ready",
                pr.get("updatedAt") or "",
            ])
    if pr_rows:
        db.table(
            headers=["project", "pr", "title", "author", "state", "updated"],
            rows=pr_rows,
        )
    else:
        msg = (
            "No open PRs found via gh CLI."
            if state["gh_available"]
            else "gh CLI not available - PR section skipped."
        )
        db.callout(msg, tone="neutral")

    db.footer(
        "operator-core::portfolio_health - rendered via templated-dashboards"
    )
    return db.build()


# ----------------------------------------------------------------------------
# Recipe
# ----------------------------------------------------------------------------

@register_recipe
class PortfolioHealth(Recipe):
    name = "portfolio_health"
    version = "1.0.0"
    description = "Daily portfolio health dashboard (HTML+MD) via templated-dashboards"
    cost_budget_usd = 0.0
    schedule = "0 8-22 * * *"   # hourly during business hours
    timeout_sec = 120
    discord_channel = None       # post is opt-in via env; default silent
    requires_clients = ()
    tags = ("daily", "ops", "dashboard")

    async def verify(self, ctx: RecipeContext) -> bool:
        """Sanity check: status dir + events dir resolvable; libs importable."""
        try:
            import dashboards  # noqa: F401  pylint: disable=import-outside-toplevel
        except Exception as exc:  # noqa: BLE001
            ctx.logger.error("portfolio_health.verify.dashboards_missing",
                             extra={"error": str(exc)})
            return False
        # Status dir is allowed not to exist (writers will create it lazily),
        # but the parent (.operator/data) must be reachable.
        events_dir = _events_dir()
        events_dir.mkdir(parents=True, exist_ok=True)
        return True

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        gh_available = _gh_logged_in()
        if not gh_available:
            ctx.logger.info("portfolio_health.gh_unavailable",
                            extra={"hint": "gh auth login"})
        return _aggregate(_projects_root(), gh_available=gh_available)

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        # No LLM. Pure roll-up.
        ctx.logger.info(
            "portfolio_health.analyze",
            extra={
                "tracked": sum(data["counts"].values()),
                "stale": len(data["stale"]),
                "runs": len(data["runs_window"]),
            },
        )
        return data

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        """Build IR + write HTML/MD outputs. Returns a short text summary
        used as the (optional) Discord message body.
        """
        from dashboards import render

        dashboard = _build_dashboard(result)

        out_dir = _war_room_dir()
        out_dir.mkdir(parents=True, exist_ok=True)
        html_path = out_dir / "portfolio-health.html"
        md_path = out_dir / "portfolio-health.md"

        # Render HTML + Markdown.
        try:
            render(dashboard, "html", out=html_path)
        except Exception as exc:  # noqa: BLE001
            ctx.logger.error("portfolio_health.html_render_failed",
                             extra={"error": str(exc)})
            raise
        try:
            render(dashboard, "markdown", out=md_path)
        except Exception as exc:  # noqa: BLE001
            ctx.logger.error("portfolio_health.md_render_failed",
                             extra={"error": str(exc)})
            raise

        # Stash the IR alongside as JSON for debugging / downstream consumers.
        ir_path = out_dir / "portfolio-health.ir.json"
        try:
            ir_path.write_text(
                json.dumps(dashboard.model_dump(exclude_none=True),
                           indent=2, ensure_ascii=True),
                encoding="utf-8",
            )
        except Exception:  # noqa: BLE001
            pass  # best-effort

        counts = result["counts"]
        summary = (
            f"Portfolio Health: {counts.get('good',0)} green / "
            f"{counts.get('warn',0)} yellow / {counts.get('bad',0)} red / "
            f"{counts.get('neutral',0)} unknown. "
            f"Stale: {len(result['stale'])}. "
            f"7d cost: ${result['total_cost_7d']:.4f}. "
            f"{html_path}"
        )
        ctx.logger.info("portfolio_health.rendered",
                        extra={"html": str(html_path), "md": str(md_path)})
        return summary

    async def post(self, ctx: RecipeContext, message: str) -> bool:
        # Default off -- writing files is the contract; Discord is an opt-in.
        if os.environ.get("PORTFOLIO_HEALTH_DISCORD", "0") in {"1", "true", "TRUE"}:
            return await super().post(ctx, message)
        return False
