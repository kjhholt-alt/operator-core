"""Project timeline normalization for Operator Cockpit.

This module turns the cockpit's existing local artifacts into a single event
stream. It does not call the network or execute external commands.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TIMELINE_EVENT_TYPES = (
    "status_snapshot",
    "pr_merged_no_review",
    "pr_merged_reviewed",
    "cost_rollup",
    "agent_checkpoint",
    "action_packet",
    "source_gap",
    "decision",
    "motion_signal",
)


def project_timeline_dir(data_dir: Path | None = None) -> Path:
    override = os.environ.get("OPERATOR_PROJECT_TIMELINE_DIR")
    if override:
        return Path(override)
    base = data_dir if data_dir is not None else Path.home() / ".operator" / "data"
    return base / "project_timelines"


def collect_project_timelines(
    *,
    state: dict[str, Any],
    output_dir: Path | None = None,
    write: bool = False,
    limit_per_project: int = 200,
) -> dict[str, Any]:
    """Return normalized timeline events, optionally materialized as JSONL.

    The returned structure is deterministic for a given cockpit state. When
    ``write`` is true, each project's current timeline snapshot is atomically
    written to ``<output_dir>/<project>.jsonl``.
    """

    events = _collect_events(state)
    events.sort(key=lambda event: (event.get("ts_sort") or "", event["id"]), reverse=True)

    by_project: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        event.pop("ts_sort", None)
        project = str(event.get("project") or "portfolio")
        by_project.setdefault(project, []).append(event)

    for project, rows in list(by_project.items()):
        by_project[project] = rows[:limit_per_project]

    latest = sorted(
        (event for rows in by_project.values() for event in rows),
        key=lambda event: (event.get("ts") or "", event["id"]),
        reverse=True,
    )[:100]

    out_dir = output_dir or project_timeline_dir()
    if write:
        write_project_timeline_snapshots(by_project, out_dir)

    counts_by_type: dict[str, int] = {}
    risk_count = 0
    for event in events:
        event_type = str(event.get("type") or "unknown")
        counts_by_type[event_type] = counts_by_type.get(event_type, 0) + 1
        if event.get("severity") in {"warn", "high"}:
            risk_count += 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dir": str(out_dir),
        "summary": {
            "project_count": len(by_project),
            "event_count": sum(len(rows) for rows in by_project.values()),
            "risk_count": risk_count,
            "counts_by_type": counts_by_type,
        },
        "projects": sorted(by_project),
        "latest": latest,
        "by_project": by_project,
    }


def write_project_timeline_snapshots(by_project: dict[str, list[dict[str, Any]]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for project, events in by_project.items():
        path = output_dir / f"{_slug(project)}.jsonl"
        lines = [json.dumps(event, sort_keys=True) for event in events]
        _atomic_write_text(path, "\n".join(lines) + ("\n" if lines else ""))


def _collect_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    events.extend(_status_events(state))
    events.extend(_weekly_review_events(state))
    events.extend(_cost_events(state))
    events.extend(_autonomy_events(state))
    events.extend(_action_packet_events(state))
    events.extend(_source_gap_events(state))
    events.extend(_decision_events(state))
    events.extend(_motion_events(state))
    return _dedupe(events)


def _status_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    statuses = state.get("statuses") if isinstance(state.get("statuses"), dict) else {}
    items = statuses.get("items") if isinstance(statuses.get("items"), list) else []
    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        health = str(item.get("health") or "unknown").lower()
        rows.append(_event(
            project=str(item.get("project") or "unknown"),
            ts=str(item.get("ts") or state.get("generated_at") or ""),
            event_type="status_snapshot",
            severity=_health_severity(health),
            title=f"Status {health}",
            summary=str(item.get("summary") or ""),
            source="status-spec",
            source_path=str(item.get("path") or ""),
            payload={"health": health},
        ))
    return rows


def _weekly_review_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    weekly = state.get("weekly_review") if isinstance(state.get("weekly_review"), dict) else {}
    rows = []
    for item in weekly.get("auto_merged", []) if isinstance(weekly.get("auto_merged"), list) else []:
        if isinstance(item, dict):
            rows.append(_pr_event(item, reviewed=False, state=state))
    for item in weekly.get("human_reviewed", []) if isinstance(weekly.get("human_reviewed"), list) else []:
        if isinstance(item, dict):
            rows.append(_pr_event(item, reviewed=True, state=state))
    return rows


def _pr_event(item: dict[str, Any], *, reviewed: bool, state: dict[str, Any]) -> dict[str, Any]:
    additions = _int(item.get("additions"))
    deletions = _int(item.get("deletions"))
    files = _int(item.get("files") if item.get("files") is not None else item.get("changed_files"))
    size = additions + deletions
    severity = "low" if reviewed else ("high" if size >= 500 or files >= 10 else "warn")
    repo = str(item.get("repo_short") or item.get("repo") or "portfolio")
    return _event(
        project=_repo_project(repo),
        ts=str(item.get("merged_at") or item.get("closed_at") or item.get("merge_time") or state.get("generated_at") or ""),
        event_type="pr_merged_reviewed" if reviewed else "pr_merged_no_review",
        severity=severity,
        title=f"PR #{item.get('number', '?')} merged" + ("" if reviewed else " without review"),
        summary=str(item.get("title") or ""),
        source="weekly-review",
        source_path=str(item.get("html_url") or ""),
        payload={
            "repo": repo,
            "number": item.get("number"),
            "additions": additions,
            "deletions": deletions,
            "files": files,
            "reviewed": reviewed,
        },
    )


def _cost_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    cost = state.get("cost") if isinstance(state.get("cost"), dict) else {}
    if not cost:
        return []
    rows = []
    generated_at = str(cost.get("generated_at") or state.get("generated_at") or "")
    total = _float(cost.get("total_30d_usd", cost.get("total_usd", 0)))
    rows.append(_event(
        project="portfolio",
        ts=generated_at,
        event_type="cost_rollup",
        severity="warn" if total >= 25 else "low",
        title="Portfolio cost rollup",
        summary=f"30d cost ${total:.2f}",
        source="portfolio-cost",
        source_path=str((state.get("artifacts") or {}).get("portfolio_cost", {}).get("path", ""))
        if isinstance(state.get("artifacts"), dict)
        else "",
        payload={
            "total_30d_usd": total,
            "by_recipe": cost.get("by_recipe") if isinstance(cost.get("by_recipe"), dict) else {},
            "by_model": cost.get("by_model") if isinstance(cost.get("by_model"), dict) else {},
            "trends": cost.get("trends") if isinstance(cost.get("trends"), dict) else {},
        },
    ))
    by_project = cost.get("by_project") if isinstance(cost.get("by_project"), dict) else {}
    for project, amount in by_project.items():
        value = _float(amount)
        rows.append(_event(
            project=str(project or "unknown"),
            ts=generated_at,
            event_type="cost_rollup",
            severity="warn" if value >= 10 else "low",
            title="Project cost rollup",
            summary=f"30d cost ${value:.2f}",
            source="portfolio-cost",
            source_path="",
            payload={"total_30d_usd": value},
        ))
    return rows


def _autonomy_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    autonomy = state.get("autonomy_evidence") if isinstance(state.get("autonomy_evidence"), dict) else {}
    latest = autonomy.get("latest") if isinstance(autonomy.get("latest"), dict) else {}
    checkpoints = latest.get("checkpoints") if isinstance(latest.get("checkpoints"), list) else []
    rows = []
    project = _project_from_mission(latest.get("mission_title") or latest.get("mission_id") or latest.get("run_id"))
    for checkpoint in checkpoints:
        if not isinstance(checkpoint, dict):
            continue
        rows.append(_event(
            project=project,
            ts=str(checkpoint.get("time") or checkpoint.get("updated_at") or latest.get("updated_at") or state.get("generated_at") or ""),
            event_type="agent_checkpoint",
            severity="warn" if str(checkpoint.get("status") or "").lower() in {"blocked", "failed"} else "low",
            title=f"Checkpoint {checkpoint.get('name') or ''}".strip(),
            summary=str(checkpoint.get("summary") or latest.get("last_summary") or ""),
            source="autonomy",
            source_path=str(checkpoint.get("path") or latest.get("path") or ""),
            payload={
                "run_id": latest.get("run_id"),
                "mission_id": latest.get("mission_id"),
                "phase": checkpoint.get("phase") or latest.get("phase"),
                "status": checkpoint.get("status") or latest.get("status"),
                "evidence": checkpoint.get("evidence") if isinstance(checkpoint.get("evidence"), list) else [],
            },
        ))
    return rows


def _action_packet_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    packets = state.get("action_packets") if isinstance(state.get("action_packets"), dict) else {}
    items = packets.get("items") if isinstance(packets.get("items"), list) else []
    rows = []
    for packet in items:
        if not isinstance(packet, dict):
            continue
        context = packet.get("context") if isinstance(packet.get("context"), dict) else {}
        paths = packet.get("paths") if isinstance(packet.get("paths"), dict) else {}
        status = str(packet.get("status") or "draft")
        rows.append(_event(
            project=_packet_project(context),
            ts=str(packet.get("updated_at") or packet.get("created_at") or state.get("generated_at") or ""),
            event_type="action_packet",
            severity="warn" if status in {"draft", "ready", "claimed"} else "low",
            title=f"Action packet {status}",
            summary=str(packet.get("title") or packet.get("kind_label") or ""),
            source="action-packets",
            source_path=str(paths.get("markdown") or paths.get("json") or ""),
            payload={
                "packet_id": packet.get("id"),
                "kind": packet.get("kind"),
                "kind_label": packet.get("kind_label"),
                "status": status,
            },
        ))
    return rows


def _source_gap_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    registry = state.get("source_registry") if isinstance(state.get("source_registry"), dict) else {}
    items = registry.get("items") if isinstance(registry.get("items"), list) else []
    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        connection = str(item.get("connection") or "")
        health = str(item.get("health") or "")
        if connection == "connected" and health == "ok":
            continue
        severity = "high" if health == "missing" else "warn"
        rows.append(_event(
            project="operator-core",
            ts=str(item.get("latest_updated_at") or state.get("generated_at") or ""),
            event_type="source_gap",
            severity=severity,
            title=f"Source {connection or health}",
            summary=str(item.get("label") or item.get("notes") or ""),
            source="source-registry",
            source_path=str(item.get("target") or ""),
            payload={
                "source_id": item.get("id"),
                "connection": connection,
                "health": health,
                "category": item.get("category"),
                "file_count": item.get("file_count"),
            },
        ))
    return rows


def _decision_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    memory = state.get("memory_learning") if isinstance(state.get("memory_learning"), dict) else {}
    decisions = memory.get("decisions") if isinstance(memory.get("decisions"), list) else []
    rows = []
    for item in decisions:
        if not isinstance(item, dict):
            continue
        title = str(item.get("decision") or item.get("title") or "Decision")
        rows.append(_event(
            project="operator-core",
            ts=str(item.get("revisit") or state.get("generated_at") or ""),
            event_type="decision",
            severity="low",
            title="Decision recorded",
            summary=title,
            source="decision-journal",
            source_path="DECISION_JOURNAL.md",
            payload=item,
        ))
    return rows


def _motion_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    motion = state.get("portfolio_motion") if isinstance(state.get("portfolio_motion"), dict) else {}
    project_motion = motion.get("project_motion") if isinstance(motion.get("project_motion"), dict) else {}
    top = project_motion.get("top_mover") if isinstance(project_motion.get("top_mover"), dict) else {}
    if not top:
        return []
    score = _int(top.get("motion_score"))
    return [_event(
        project=_repo_project(str(top.get("slug") or top.get("title") or "portfolio")),
        ts=str(top.get("latest_update") or motion.get("generated_at") or state.get("generated_at") or ""),
        event_type="motion_signal",
        severity="warn" if score >= 80 else "low",
        title="Top portfolio mover",
        summary=str(top.get("next_action") or top.get("title") or ""),
        source="project-motion",
        source_path="project-motion-board.json",
        payload=top,
    )]


def _event(
    *,
    project: str,
    ts: str,
    event_type: str,
    severity: str,
    title: str,
    summary: str,
    source: str,
    source_path: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    project = _slug(project or "portfolio")
    normalized_ts = _normalize_ts(ts)
    event = {
        "id": "",
        "project": project,
        "ts": normalized_ts or ts,
        "type": event_type if event_type in TIMELINE_EVENT_TYPES else "unknown",
        "severity": severity if severity in {"low", "warn", "high"} else "low",
        "title": title,
        "summary": summary,
        "source": source,
        "source_path": source_path,
        "payload": payload,
        "ts_sort": normalized_ts or ts or "",
    }
    event["id"] = _event_id(event)
    return event


def _dedupe(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for event in events:
        event_id = event["id"]
        if event_id in seen:
            continue
        seen.add(event_id)
        out.append(event)
    return out


def _event_id(event: dict[str, Any]) -> str:
    stable = {
        "project": event.get("project"),
        "ts": event.get("ts"),
        "type": event.get("type"),
        "title": event.get("title"),
        "summary": event.get("summary"),
        "source": event.get("source"),
        "source_path": event.get("source_path"),
        "payload": event.get("payload"),
    }
    digest = hashlib.sha1(json.dumps(stable, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:12]
    return f"{event.get('project')}-{event.get('type')}-{digest}"


def _packet_project(context: dict[str, Any]) -> str:
    explicit = context.get("project") or context.get("repo")
    if explicit:
        return _repo_project(str(explicit))
    motion = context.get("portfolio_motion") if isinstance(context.get("portfolio_motion"), dict) else {}
    top = motion.get("top_mover") if isinstance(motion.get("top_mover"), dict) else {}
    if top.get("slug") or top.get("title"):
        return _repo_project(str(top.get("slug") or top.get("title")))
    mission = context.get("mission") if isinstance(context.get("mission"), dict) else {}
    return _project_from_mission(mission.get("title") or mission.get("current_run") or "operator-core")


def _project_from_mission(value: Any) -> str:
    text = str(value or "").lower()
    known = (
        "operator-core",
        "war-room",
        "portfolio",
        "prospector-pro",
        "ai-ops-consulting",
        "templated-dashboards",
        "status-spec",
    )
    for slug in known:
        if slug in text or slug.replace("-", " ") in text:
            return slug
    return "operator-core"


def _repo_project(repo: str) -> str:
    repo = repo.rsplit("/", 1)[-1]
    return _slug(repo)


def _health_severity(health: str) -> str:
    if health in {"red", "error", "failed", "fail"}:
        return "high"
    if health in {"yellow", "warn", "warning", "unknown"}:
        return "warn"
    return "low"


def _normalize_ts(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text + "T00:00:00+00:00"
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", str(value).lower()).strip("-")
    return slug or "portfolio"


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _atomic_write_text(path: Path, body: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(body, encoding="utf-8")
    tmp.replace(path)
