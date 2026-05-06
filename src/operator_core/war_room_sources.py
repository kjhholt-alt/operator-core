"""War-room source registry for cockpit migration parity.

The registry is deliberately read-only. It inventories the files and folders
that make up the existing war-room operating system so the live cockpit can
show what is already connected, what is still static-only, and what would be
lost if the old surfaces were retired too early.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

ConnectionState = Literal["connected", "static-only", "not-connected"]


@dataclass(frozen=True)
class SourceSpec:
    id: str
    label: str
    category: str
    owner: str
    patterns: tuple[str, ...]
    schema: str
    target: str
    connection: ConnectionState
    freshness_hours: int | None = 168
    notes: str = ""


SOURCE_SPECS: tuple[SourceSpec, ...] = (
    SourceSpec(
        id="status-docs",
        label="Status-spec docs",
        category="portfolio",
        owner="operator-core",
        patterns=("status/*.json",),
        schema="status-spec/v1 json",
        target="Portfolio health / Status stream",
        connection="connected",
        freshness_hours=2,
        notes="Canonical project health and cost status documents.",
    ),
    SourceSpec(
        id="portfolio-health",
        label="Portfolio health IR",
        category="portfolio",
        owner="operator-core",
        patterns=("war-room/portfolio-health.ir.json",),
        schema="templated-dashboards IR json",
        target="Cockpit portfolio health",
        connection="connected",
        freshness_hours=24,
    ),
    SourceSpec(
        id="morning-briefing",
        label="Morning briefing",
        category="briefing",
        owner="operator-core",
        patterns=("war-room/morning.md",),
        schema="markdown",
        target="Cockpit briefing",
        connection="connected",
        freshness_hours=24,
    ),
    SourceSpec(
        id="weekly-review",
        label="Weekly autonomous merge review",
        category="review",
        owner="operator-core",
        patterns=("war-room/weekly-review.json", "war-room/weekly-review.html"),
        schema="json + archived html",
        target="Cockpit weekly review",
        connection="connected",
        freshness_hours=192,
    ),
    SourceSpec(
        id="portfolio-cost",
        label="Portfolio cost rollup",
        category="cost",
        owner="operator-core",
        patterns=("data/portfolio_cost.json",),
        schema="json",
        target="Cockpit cost rollup",
        connection="connected",
        freshness_hours=24,
    ),
    SourceSpec(
        id="active-mission",
        label="Active mission",
        category="mission",
        owner="war-room",
        patterns=(
            "war-room/ACTIVE_MISSION.md",
            "war-room/current-run.json",
            "war-room/autonomy/missions/active.json",
        ),
        schema="markdown + json",
        target="Mission Control",
        connection="connected",
        freshness_hours=48,
        notes="Core daily continuity surface; should be first migration target.",
    ),
    SourceSpec(
        id="next-agent-card",
        label="Next-agent execution card",
        category="mission",
        owner="war-room",
        patterns=("war-room/NEXT_AGENT_CARD.md", "war-room/SOURCE_ACTIONS.md"),
        schema="markdown",
        target="Mission Control",
        connection="connected",
        freshness_hours=72,
    ),
    SourceSpec(
        id="agent-launch-queue",
        label="Agent launch queue",
        category="agents",
        owner="war-room",
        patterns=("war-room/agent-launch-queue.json", "war-room/agent-launch-queue.html"),
        schema="json + static html",
        target="Agent Queue",
        connection="connected",
        freshness_hours=168,
    ),
    SourceSpec(
        id="agent-handoff-board",
        label="Agent handoff board",
        category="agents",
        owner="war-room",
        patterns=(
            "war-room/agent-handoff-board.json",
            "war-room/handoffs/OPEN_QUEUE.md",
            "war-room/handoffs/CODEX.md",
        ),
        schema="json + markdown",
        target="Agent Queue",
        connection="connected",
        freshness_hours=168,
    ),
    SourceSpec(
        id="autonomy-runs",
        label="Autonomy run ledger and evidence",
        category="autonomy",
        owner="war-room",
        patterns=(
            "war-room/autonomy/run-index.jsonl",
            "war-room/autonomy/runs/*/evidence.jsonl",
            "war-room/autonomy/runs/*/checkpoint.json",
            "war-room/autonomy/runs/*/resume.md",
        ),
        schema="jsonl + json + markdown",
        target="Autonomy Evidence",
        connection="connected",
        freshness_hours=72,
    ),
    SourceSpec(
        id="memory-os",
        label="Memory OS",
        category="memory",
        owner="war-room",
        patterns=("war-room/memory-os.json", "war-room/DECISION_JOURNAL.md"),
        schema="json + markdown",
        target="Memory / Decisions",
        connection="connected",
        freshness_hours=336,
    ),
    SourceSpec(
        id="learning-loop",
        label="Learning loop and flow library",
        category="memory",
        owner="war-room",
        patterns=(
            "war-room/learning-loop.json",
            "war-room/FLOW_LIBRARY.md",
            "war-room/FLOW_RECOMMENDATION.md",
            "war-room/FLOW_LESSONS.md",
        ),
        schema="json + markdown",
        target="Learning",
        connection="connected",
        freshness_hours=336,
    ),
    SourceSpec(
        id="project-motion",
        label="Project motion board",
        category="portfolio",
        owner="war-room",
        patterns=("war-room/project-motion-board.json", "war-room/PROJECT_MOTION_BOARD.md"),
        schema="json + markdown",
        target="Project Motion",
        connection="connected",
        freshness_hours=168,
    ),
    SourceSpec(
        id="side-project-os",
        label="Side-project portfolio OS",
        category="portfolio",
        owner="war-room",
        patterns=(
            "war-room/side-projects-portfolio-os.json",
            "war-room/SIDE_PROJECTS_PORTFOLIO_OS.md",
            "war-room/handoffs/side-projects/OPEN_QUEUE.md",
        ),
        schema="json + markdown",
        target="Side Projects",
        connection="connected",
        freshness_hours=336,
    ),
    SourceSpec(
        id="forge",
        label="Forge / prototype garden",
        category="forge",
        owner="war-room",
        patterns=(
            "war-room/forge.json",
            "war-room/forge-runs.jsonl",
            "war-room/PROTOTYPE_GARDEN.md",
            "war-room/EXPERIMENT_ARCADE.md",
            "war-room/NEXT_BUILD_CARD.md",
        ),
        schema="json + jsonl + markdown",
        target="Forge",
        connection="connected",
        freshness_hours=336,
    ),
    SourceSpec(
        id="skills-arena",
        label="Skills / Arena",
        category="skills",
        owner="war-room",
        patterns=(
            "war-room/kruz-skills.json",
            "war-room/kruz-skill-runs.jsonl",
            "war-room/kruz-skill-proposals.jsonl",
            "war-room/AGENT_SKILL_GYM.md",
        ),
        schema="json + jsonl + markdown",
        target="Skills",
        connection="connected",
        freshness_hours=336,
    ),
    SourceSpec(
        id="evaluations",
        label="Evaluations and QA",
        category="quality",
        owner="war-room",
        patterns=(
            "war-room/evaluations/*.md",
            "war-room/all-pages-qa.json",
            "war-room/GAUNTLET_REPORT.md",
            "war-room/gauntlet-report.json",
        ),
        schema="markdown + json",
        target="Evaluation / QA",
        connection="connected",
        freshness_hours=336,
    ),
    SourceSpec(
        id="run-history",
        label="Run logs and replay theater",
        category="history",
        owner="war-room",
        patterns=(
            "war-room/RUNLOG.md",
            "war-room/agent-runs.jsonl",
            "war-room/run-replay-theater.html",
            "war-room/RUN_REPLAY_THEATER.md",
        ),
        schema="markdown + jsonl + static html",
        target="Run History",
        connection="connected",
        freshness_hours=336,
    ),
    SourceSpec(
        id="legacy-war-room-dashboard",
        label="Legacy war-room dashboard",
        category="legacy-html",
        owner="war-room",
        patterns=("war-room/index.html", "war-room/*.html"),
        schema="static html",
        target="Archive / embedded fallback",
        connection="static-only",
        freshness_hours=None,
        notes="Keep until equivalent cockpit sections exist.",
    ),
    SourceSpec(
        id="streamlit-cockpit",
        label="Existing Streamlit cockpit",
        category="legacy-app",
        owner="war-room",
        patterns=("war-room/cockpit/cockpit_app.py", "war-room/cockpit/cockpit_state.py"),
        schema="streamlit app",
        target="Feature parity reference",
        connection="static-only",
        freshness_hours=None,
        notes="This is the richest current cockpit; use as migration reference.",
    ),
    SourceSpec(
        id="operator-portfolio-routes",
        label="Operator portfolio HTTP routes",
        category="operator-core",
        owner="operator-core",
        patterns=("repo/src/operator_core/portfolio_routes.py",),
        schema="http routes",
        target="/portfolio /priorities /blocked",
        connection="connected",
        freshness_hours=None,
        notes="Registered by daemon so cockpit can link to portfolio APIs.",
    ),
)


def collect_source_registry(
    *,
    war_room_dir: Path,
    data_dir: Path,
    status_dir: Path,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    repo_root = repo_root or Path(__file__).resolve().parents[2]
    roots = {
        "war-room": war_room_dir,
        "data": data_dir,
        "status": status_dir,
        "repo": repo_root,
    }
    now = datetime.now(timezone.utc)
    items = [_collect_one(spec, roots, now) for spec in SOURCE_SPECS]
    summary: dict[str, int] = {
        "total": len(items),
        "connected": 0,
        "static_only": 0,
        "not_connected": 0,
        "missing": 0,
        "stale": 0,
        "ok": 0,
    }
    by_category: dict[str, int] = {}
    for item in items:
        connection_key = item["connection"].replace("-", "_")
        summary[connection_key] = summary.get(connection_key, 0) + 1
        summary[item["health"]] = summary.get(item["health"], 0) + 1
        by_category[item["category"]] = by_category.get(item["category"], 0) + 1
    missing_connections = [
        item for item in items if item["connection"] == "not-connected" and item["exists"]
    ]
    missing_connections.sort(key=lambda item: (item["health"] != "ok", item["category"], item["label"]))
    return {
        "generated_at": now.isoformat(),
        "summary": summary,
        "by_category": by_category,
        "items": items,
        "missing_connections": missing_connections,
    }


def _collect_one(spec: SourceSpec, roots: dict[str, Path], now: datetime) -> dict[str, Any]:
    matches: list[Path] = []
    declared_paths: list[str] = []
    for pattern in spec.patterns:
        root_key, rel = _split_pattern(pattern)
        root = roots[root_key]
        declared_paths.append(str(root / rel))
        if _has_glob(rel):
            matches.extend(p for p in root.glob(rel) if p.exists())
        else:
            path = root / rel
            if path.exists():
                matches.append(path)

    unique_matches = sorted({p.resolve() for p in matches})
    files = [_path_meta(path, now) for path in unique_matches[:25]]
    latest = max((item for item in files if item.get("updated_at")), key=lambda item: item["updated_at"], default=None)
    exists = bool(files)
    age_hours = latest.get("age_hours") if latest else None
    stale = bool(
        exists
        and spec.freshness_hours is not None
        and age_hours is not None
        and age_hours > spec.freshness_hours
    )
    health = "missing" if not exists else ("stale" if stale else "ok")
    return {
        "id": spec.id,
        "label": spec.label,
        "category": spec.category,
        "owner": spec.owner,
        "connection": spec.connection,
        "health": health,
        "exists": exists,
        "stale": stale,
        "schema": spec.schema,
        "target": spec.target,
        "freshness_hours": spec.freshness_hours,
        "notes": spec.notes,
        "declared_paths": declared_paths,
        "file_count": len(unique_matches),
        "files": files,
        "latest_updated_at": latest.get("updated_at") if latest else None,
        "latest_age_hours": age_hours,
        "latest_path": latest.get("path") if latest else None,
    }


def _split_pattern(pattern: str) -> tuple[str, str]:
    prefix, _, rest = pattern.partition("/")
    if prefix not in {"war-room", "data", "status", "repo"}:
        return "war-room", pattern
    return prefix, rest


def _has_glob(pattern: str) -> bool:
    return any(ch in pattern for ch in "*?[]")


def _path_meta(path: Path, now: datetime) -> dict[str, Any]:
    try:
        stat = path.stat()
        updated = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
        age_hours = round((now - updated).total_seconds() / 3600, 2)
        return {
            "path": str(path),
            "name": path.name,
            "kind": "dir" if path.is_dir() else "file",
            "size": stat.st_size,
            "updated_at": updated.isoformat(),
            "age_hours": age_hours,
        }
    except OSError as exc:
        return {
            "path": str(path),
            "name": path.name,
            "kind": "unknown",
            "size": 0,
            "updated_at": None,
            "age_hours": None,
            "error": str(exc),
        }
