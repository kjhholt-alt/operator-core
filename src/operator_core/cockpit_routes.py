"""Live Operator Cockpit routes.

The cockpit reads structured artifacts produced by scheduled recipes and
renders one local app surface instead of linking out to standalone HTML
reports. It is dependency-free and local-only through the shared HTTP server.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .http_server import register_extra_route
from .war_room_agents import collect_agent_coordination
from .war_room_autonomy import collect_autonomy_evidence
from .war_room_memory import collect_memory_learning
from .war_room_mission import collect_mission_control
from .war_room_sources import collect_source_registry


def _safe_settings():
    try:
        from .settings import load_settings

        return load_settings()
    except Exception:
        return None


def _data_dir() -> Path:
    override = os.environ.get("OPERATOR_DATA_DIR")
    if override:
        return Path(override)
    settings = _safe_settings()
    if settings is not None:
        return settings.data_dir
    return Path.home() / ".operator" / "data"


def _projects_root() -> Path:
    override = os.environ.get("OPERATOR_PROJECTS_DIR")
    if override:
        return Path(override)
    settings = _safe_settings()
    if settings is not None:
        return settings.projects_root
    candidate = Path.home() / "Desktop" / "Projects"
    return candidate if candidate.exists() else Path.cwd()


def _war_room_dir() -> Path:
    override = os.environ.get("OPERATOR_WAR_ROOM_DIR")
    if override:
        return Path(override)
    return _projects_root() / "war-room"


def _status_dir() -> Path:
    override = os.environ.get("OPERATOR_STATUS_DIR")
    if override:
        return Path(override)
    return _data_dir() / "status"


def _read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _read_text(path: Path, default: str = "") -> str:
    try:
        if not path.exists():
            return default
        return path.read_text(encoding="utf-8")
    except OSError:
        return default


def _artifact_meta(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
    except OSError:
        return {"path": str(path), "exists": False, "updated_at": None, "size": 0}
    return {
        "path": str(path),
        "exists": True,
        "updated_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        "size": stat.st_size,
    }


def _status_docs() -> list[dict[str, Any]]:
    root = _status_dir()
    docs: list[dict[str, Any]] = []
    if not root.exists():
        return docs
    for path in sorted(root.glob("*.json")):
        doc = _read_json(path, {})
        if not isinstance(doc, dict):
            continue
        docs.append({
            "project": str(doc.get("project") or path.stem),
            "health": str(doc.get("health") or doc.get("status") or "unknown"),
            "summary": str(doc.get("summary") or doc.get("error") or ""),
            "ts": str(doc.get("ts") or doc.get("last_run") or ""),
            "path": str(path),
        })
    return docs


def _portfolio_from_ir(ir: dict[str, Any]) -> dict[str, Any]:
    overview = {}
    sections = ir.get("sections") if isinstance(ir, dict) else []
    if isinstance(sections, list):
        for section in sections:
            if not isinstance(section, dict) or section.get("title") != "Overview":
                continue
            for component in section.get("components") or []:
                if isinstance(component, dict) and component.get("type") == "kpi_tile":
                    overview[str(component.get("label") or "?")] = component.get("value", 0)
    return {
        "title": ir.get("title", "Portfolio Health") if isinstance(ir, dict) else "Portfolio Health",
        "subtitle": ir.get("subtitle", "") if isinstance(ir, dict) else "",
        "overview": overview,
    }


def collect_cockpit_state() -> dict[str, Any]:
    war_room = _war_room_dir()
    data_dir = _data_dir()
    status_dir = _status_dir()
    portfolio_ir_path = war_room / "portfolio-health.ir.json"
    morning_md_path = war_room / "morning.md"
    weekly_json_path = war_room / "weekly-review.json"
    cost_path = Path(os.environ.get("OPERATOR_PORTFOLIO_COST_PATH", str(data_dir / "portfolio_cost.json")))

    portfolio_ir = _read_json(portfolio_ir_path, {})
    weekly_review = _read_json(weekly_json_path, {})
    portfolio_cost = _read_json(cost_path, {})
    morning_md = _read_text(morning_md_path)
    statuses = _status_docs()
    mission_control = collect_mission_control(war_room)
    agent_coordination = collect_agent_coordination(war_room)
    autonomy_evidence = collect_autonomy_evidence(war_room)
    memory_learning = collect_memory_learning(war_room)
    source_registry = collect_source_registry(
        war_room_dir=war_room,
        data_dir=data_dir,
        status_dir=status_dir,
    )

    health_counts = {"green": 0, "yellow": 0, "red": 0, "unknown": 0}
    for doc in statuses:
        health = doc["health"].lower()
        health_counts[health if health in health_counts else "unknown"] += 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "paths": {
            "war_room": str(war_room),
            "data_dir": str(data_dir),
            "status_dir": str(status_dir),
        },
        "artifacts": {
            "portfolio_health": _artifact_meta(portfolio_ir_path),
            "morning": _artifact_meta(morning_md_path),
            "weekly_review": _artifact_meta(weekly_json_path),
            "portfolio_cost": _artifact_meta(cost_path),
        },
        "portfolio": _portfolio_from_ir(portfolio_ir if isinstance(portfolio_ir, dict) else {}),
        "morning": {
            "markdown": morning_md,
            "preview": "\n".join(morning_md.splitlines()[:16]),
        },
        "mission_control": mission_control,
        "agent_coordination": agent_coordination,
        "autonomy_evidence": autonomy_evidence,
        "memory_learning": memory_learning,
        "weekly_review": weekly_review if isinstance(weekly_review, dict) else {},
        "cost": portfolio_cost if isinstance(portfolio_cost, dict) else {},
        "source_registry": source_registry,
        "statuses": {
            "count": len(statuses),
            "health_counts": health_counts,
            "items": statuses,
        },
    }


def _esc(value: Any) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _money(value: Any) -> str:
    try:
        return f"${float(value):.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def _stat(label: str, value: Any, tone: str = "") -> str:
    return (
        f'<div class="metric {tone}">'
        f'<div class="metric-label">{_esc(label)}</div>'
        f'<div class="metric-value">{_esc(value)}</div>'
        "</div>"
    )


def _status_rows(items: list[dict[str, Any]]) -> str:
    rows = []
    for item in items[:40]:
        health = str(item.get("health") or "unknown").lower()
        rows.append(
            "<tr>"
            f'<td><span class="dot {health}"></span>{_esc(item.get("project"))}</td>'
            f'<td>{_esc(item.get("health"))}</td>'
            f'<td>{_esc(item.get("summary"))}</td>'
            f'<td class="mono">{_esc(item.get("ts"))}</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="4" class="empty">No status docs found.</td></tr>'


def _cost_rows(cost: dict[str, Any]) -> str:
    by_recipe = cost.get("by_recipe") if isinstance(cost.get("by_recipe"), dict) else {}
    rows = []
    for name, total in sorted(by_recipe.items(), key=lambda kv: float(kv[1] or 0), reverse=True)[:12]:
        rows.append(f"<tr><td>{_esc(name)}</td><td class=\"right mono\">{_money(total)}</td></tr>")
    return "\n".join(rows) or '<tr><td colspan="2" class="empty">No cost rollup yet.</td></tr>'


def _weekly_rows(weekly: dict[str, Any]) -> str:
    rows = []
    for pr in weekly.get("auto_merged", [])[:12] if isinstance(weekly.get("auto_merged"), list) else []:
        rows.append(
            "<tr>"
            f'<td>{_esc(pr.get("repo_short") or pr.get("repo"))}</td>'
            f'<td class="mono">#{_esc(pr.get("number"))}</td>'
            f'<td>{_esc(pr.get("title"))}</td>'
            f'<td class="right mono">+{_esc(pr.get("additions", 0))}/-{_esc(pr.get("deletions", 0))}</td>'
            f'<td class="right mono">{_esc(pr.get("files", 0))}</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="5" class="empty">No auto-merged PR data yet.</td></tr>'


def _list_items(items: list[Any], limit: int = 6) -> str:
    rows = [f"<li>{_esc(item)}</li>" for item in items[:limit]]
    return "\n".join(rows) or '<li class="empty-inline">None found.</li>'


def _source_action_rows(cards: list[dict[str, Any]]) -> str:
    rows = []
    for card in cards[:8]:
        rows.append(
            "<tr>"
            f'<td>{_esc(card.get("product") or card.get("title"))}</td>'
            f'<td class="mono">{_esc(card.get("source"))}</td>'
            f'<td>{_esc(card.get("status"))}</td>'
            f'<td>{_esc(card.get("first_check") or card.get("issue"))}</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="4" class="empty">No source action cards found.</td></tr>'


def _launch_rows(missions: list[dict[str, Any]]) -> str:
    rows = []
    for mission in missions[:8]:
        rows.append(
            "<tr>"
            f'<td>{_esc(mission.get("title"))}<div class="subtle">{_esc(mission.get("why"))}</div></td>'
            f'<td>{_esc(mission.get("agent"))}</td>'
            f'<td>{_esc(mission.get("status"))}</td>'
            f'<td class="right mono">{_esc(mission.get("rank_score"))}</td>'
            f'<td class="right mono">{_esc(mission.get("duration_minutes"))}m</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="5" class="empty">No launch queue missions found.</td></tr>'


def _handoff_rows(missions: list[dict[str, Any]]) -> str:
    rows = []
    for mission in missions[:10]:
        rows.append(
            "<tr>"
            f'<td>{_esc(mission.get("title"))}<div class="subtle">{_esc(mission.get("output"))}</div></td>'
            f'<td>{_esc(mission.get("agent"))}</td>'
            f'<td>{_esc(mission.get("status"))}</td>'
            f'<td class="right mono">{_esc(mission.get("priority"))}</td>'
            f'<td>{_esc(", ".join(mission.get("write_scope") or []))}</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="5" class="empty">No handoff missions found.</td></tr>'


def _claim_cards(claims: list[dict[str, Any]]) -> str:
    cards = []
    for claim in claims:
        status = str(claim.get("status") or "unknown").lower()
        cards.append(
            f'<div class="claim {status}">'
            f'<div class="claim-agent">{_esc(claim.get("agent"))}</div>'
            f'<div class="claim-status">{_esc(claim.get("status") or "unknown")}</div>'
            f'<div class="subtle mono">{_esc(claim.get("mission_id") or "free")}</div>'
            "</div>"
        )
    return "\n".join(cards) or '<div class="empty">No claim slots found.</div>'


def _score_pills(score: dict[str, Any]) -> str:
    if not score:
        return '<span class="subtle">Not scored</span>'
    return " ".join(
        f'<span class="score-pill">{_esc(key)} {_esc(value)}</span>'
        for key, value in sorted(score.items())
    )


def _checkpoint_rows(checkpoints: list[dict[str, Any]]) -> str:
    rows = []
    for checkpoint in reversed(checkpoints[-6:]):
        rows.append(
            "<tr>"
            f'<td class="mono">{_esc(checkpoint.get("name"))}</td>'
            f'<td>{_esc(checkpoint.get("phase"))}</td>'
            f'<td>{_esc(checkpoint.get("status"))}</td>'
            f'<td>{_esc(checkpoint.get("summary"))}</td>'
            f'<td class="mono small">{_esc(checkpoint.get("time"))}</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="5" class="empty">No checkpoints found.</td></tr>'


def _evidence_rows(evidence: list[dict[str, Any]]) -> str:
    rows = []
    for item in reversed(evidence[-8:]):
        rows.append(
            "<tr>"
            f'<td class="mono small">{_esc(item.get("ts"))}</td>'
            f'<td>{_esc(item.get("phase"))}</td>'
            f'<td>{_esc(item.get("evidence") or item.get("raw") or item)}</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="3" class="empty">No evidence found.</td></tr>'


def _flow_rows(patterns: list[dict[str, Any]]) -> str:
    rows = []
    for item in patterns[:8]:
        rows.append(
            "<tr>"
            f'<td>{_esc(item.get("flow"))}</td>'
            f'<td class="right mono">{_esc(item.get("run_count", 0))}</td>'
            f'<td class="right mono">{_esc(item.get("average_score", ""))}</td>'
            f'<td>{_esc(item.get("best_mission"))}</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="4" class="empty">No flow patterns found.</td></tr>'


def _decision_rows(decisions: list[dict[str, Any]]) -> str:
    rows = []
    for item in decisions[:6]:
        rows.append(
            "<tr>"
            f'<td>{_esc(item.get("decision"))}</td>'
            f'<td>{_esc(item.get("why"))}</td>'
            f'<td>{_esc(item.get("revisit"))}</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="3" class="empty">No decision entries found.</td></tr>'


def _document_rows(documents: list[dict[str, Any]]) -> str:
    rows = []
    for item in documents[:8]:
        signals = item.get("signals") if isinstance(item.get("signals"), list) else []
        rows.append(
            "<tr>"
            f'<td>{_esc(item.get("title"))}<div class="subtle mono">{_esc(item.get("relative_path"))}</div></td>'
            f'<td>{_esc(item.get("kind"))}</td>'
            f'<td>{_esc(", ".join(str(s) for s in signals[:4]))}</td>'
            f'<td class="mono small">{_esc(item.get("mtime"))}</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="4" class="empty">No memory documents found.</td></tr>'


def _source_rows(items: list[dict[str, Any]]) -> str:
    order = {"not-connected": 0, "static-only": 1, "connected": 2}
    rows = []
    for item in sorted(items, key=lambda x: (order.get(str(x.get("connection")), 9), str(x.get("category")), str(x.get("label")))):
        latest = item.get("latest_updated_at") or "missing"
        if item.get("latest_age_hours") is not None:
            latest = f'{latest} ({item.get("latest_age_hours")}h)'
        rows.append(
            "<tr>"
            f'<td><span class="badge { _esc(item.get("connection")) }">{_esc(item.get("connection"))}</span></td>'
            f'<td>{_esc(item.get("label"))}<div class="subtle">{_esc(item.get("notes"))}</div></td>'
            f'<td>{_esc(item.get("category"))}</td>'
            f'<td>{_esc(item.get("target"))}</td>'
            f'<td><span class="health { _esc(item.get("health")) }">{_esc(item.get("health"))}</span></td>'
            f'<td class="right mono">{_esc(item.get("file_count"))}</td>'
            f'<td class="mono small">{_esc(latest)}</td>'
            "</tr>"
        )
    return "\n".join(rows) or '<tr><td colspan="7" class="empty">No source registry entries.</td></tr>'


def render_cockpit(state: dict[str, Any]) -> str:
    portfolio = state["portfolio"]
    overview = portfolio.get("overview") or {}
    statuses = state["statuses"]
    health = statuses["health_counts"]
    cost = state["cost"]
    trends = cost.get("trends") if isinstance(cost.get("trends"), dict) else {}
    weekly = state["weekly_review"]
    auto_count = len(weekly.get("auto_merged", [])) if isinstance(weekly.get("auto_merged"), list) else 0
    reviewed_count = len(weekly.get("human_reviewed", [])) if isinstance(weekly.get("human_reviewed"), list) else 0
    artifacts = state["artifacts"]
    mission_control = state.get("mission_control") if isinstance(state.get("mission_control"), dict) else {}
    agent_coordination = state.get("agent_coordination") if isinstance(state.get("agent_coordination"), dict) else {}
    autonomy_evidence = state.get("autonomy_evidence") if isinstance(state.get("autonomy_evidence"), dict) else {}
    memory_learning = state.get("memory_learning") if isinstance(state.get("memory_learning"), dict) else {}
    memory = memory_learning.get("memory") if isinstance(memory_learning.get("memory"), dict) else {}
    learning = memory_learning.get("learning") if isinstance(memory_learning.get("learning"), dict) else {}
    flow_rec = memory_learning.get("flow_recommendation") if isinstance(memory_learning.get("flow_recommendation"), dict) else {}
    flow_lessons = memory_learning.get("flow_lessons") if isinstance(memory_learning.get("flow_lessons"), dict) else {}
    latest_run = autonomy_evidence.get("latest") if isinstance(autonomy_evidence.get("latest"), dict) else {}
    latest_checkpoint = latest_run.get("latest_checkpoint") if isinstance(latest_run.get("latest_checkpoint"), dict) else {}
    launch_queue = agent_coordination.get("launch_queue") if isinstance(agent_coordination.get("launch_queue"), dict) else {}
    handoff_board = agent_coordination.get("handoff_board") if isinstance(agent_coordination.get("handoff_board"), dict) else {}
    launch_missions = launch_queue.get("missions") if isinstance(launch_queue.get("missions"), list) else []
    handoff_missions = handoff_board.get("missions") if isinstance(handoff_board.get("missions"), list) else []
    claims = handoff_board.get("claims") if isinstance(handoff_board.get("claims"), list) else []
    top_launch = launch_queue.get("top_mission") if isinstance(launch_queue.get("top_mission"), dict) else {}
    mission = mission_control.get("mission") if isinstance(mission_control.get("mission"), dict) else {}
    current_run = mission_control.get("current_run") if isinstance(mission_control.get("current_run"), dict) else {}
    active_autonomy = mission_control.get("active_autonomy") if isinstance(mission_control.get("active_autonomy"), dict) else {}
    next_agent = mission_control.get("next_agent") if isinstance(mission_control.get("next_agent"), dict) else {}
    source_actions = mission_control.get("source_actions") if isinstance(mission_control.get("source_actions"), dict) else {}
    source_cards = source_actions.get("cards") if isinstance(source_actions.get("cards"), list) else []
    registry = state.get("source_registry") if isinstance(state.get("source_registry"), dict) else {}
    registry_summary = registry.get("summary") if isinstance(registry.get("summary"), dict) else {}
    missing_connections = registry.get("missing_connections") if isinstance(registry.get("missing_connections"), list) else []

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Operator Cockpit</title>
<style>{_CSS}</style>
</head>
<body>
<div class="shell">
  <aside class="rail">
    <div class="brand">OPERATOR<br><span>COCKPIT</span></div>
    <a href="#mission">Mission</a>
    <a href="#agents">Agents</a>
    <a href="#autonomy">Autonomy</a>
    <a href="#memory">Memory</a>
    <a href="#portfolio">Portfolio</a>
    <a href="#briefing">Briefing</a>
    <a href="#review">Review</a>
    <a href="#costs">Costs</a>
    <a href="#sources">Sources</a>
    <a href="#status">Status</a>
    <a href="/cockpit.json">JSON</a>
  </aside>
  <main>
    <header class="topbar">
      <div>
        <h1>Operator Cockpit</h1>
        <p class="muted mono">Last refresh <span id="last-refresh">{_esc(state["generated_at"])}</span></p>
      </div>
      <div class="links"><a href="/ops">Ops</a><a href="/gate-review">Gate Review</a><a href="/cut-over">Cut-over</a></div>
    </header>

    <section class="metrics" id="portfolio">
      {_stat("Tracked Status", statuses["count"])}
      {_stat("Source Actions", source_actions.get("count", 0), "warn" if source_actions.get("count", 0) else "")}
      {_stat("Open Handoffs", handoff_board.get("open_count", 0), "warn" if handoff_board.get("open_count", 0) else "")}
      {_stat("Checkpoints", latest_run.get("checkpoint_count", 0), "")}
      {_stat("Memory Docs", (memory.get("summary") or {}).get("indexed_documents", 0) if isinstance(memory.get("summary"), dict) else 0, "")}
      {_stat("Green", health.get("green", 0), "good")}
      {_stat("Yellow", health.get("yellow", 0), "warn")}
      {_stat("Red", health.get("red", 0), "bad")}
      {_stat("Auto-Merged", auto_count, "warn" if auto_count else "")}
      {_stat("Week Cost", _money(trends.get("week_current_usd")), "")}
      {_stat("Unconnected Sources", registry_summary.get("not_connected", 0), "warn" if registry_summary.get("not_connected", 0) else "")}
    </section>

    <section class="panel" id="memory">
      <div class="panel-head"><h2>Memory + Learning</h2><span>{_esc(memory_learning.get("generated_at", ""))}</span></div>
      <div class="memory-grid">
        <div>
          <div class="eyebrow">Recommended Flow</div>
          <h3>{_esc(flow_rec.get("flow") or "No recommendation")}</h3>
          <p>{_esc(flow_rec.get("mission") or "No mission found.")}</p>
          <div class="run-strip">
            <span><strong>Confidence</strong> {_esc(flow_rec.get("confidence") or "unknown")}</span>
            <span><strong>Score</strong> {_esc(flow_rec.get("average_flow_score") or "unknown")}</span>
          </div>
          <ul class="tight-list">{_list_items(flow_rec.get("run_contract", []) if isinstance(flow_rec.get("run_contract"), list) else [], 4)}</ul>
        </div>
        <div>
          <div class="eyebrow">Latest Replay</div>
          <h3>{_esc((learning.get("latest_run") or {}).get("mission") if isinstance(learning.get("latest_run"), dict) else "No replay")}</h3>
          <p>{_esc((learning.get("latest_run") or {}).get("verdict") if isinstance(learning.get("latest_run"), dict) else "")}</p>
          <div class="run-strip">
            <span><strong>Flow</strong> {_esc((learning.get("latest_run") or {}).get("flow") if isinstance(learning.get("latest_run"), dict) else "")}</span>
            <span><strong>Grade</strong> {_esc((learning.get("latest_run") or {}).get("grade") if isinstance(learning.get("latest_run"), dict) else "")}</span>
            <span><strong>Runs</strong> {_esc(learning.get("run_count", 0))}</span>
          </div>
        </div>
        <div>
          <div class="eyebrow">Memory OS</div>
          <h3>{_esc(memory.get("version") or "Memory index")}</h3>
          <div class="score-row">
            <span class="score-pill">projects {_esc((memory.get("summary") or {}).get("projects", 0) if isinstance(memory.get("summary"), dict) else 0)}</span>
            <span class="score-pill">decisions {_esc((memory.get("summary") or {}).get("decisions", 0) if isinstance(memory.get("summary"), dict) else 0)}</span>
            <span class="score-pill">docs {_esc((memory.get("summary") or {}).get("indexed_documents", 0) if isinstance(memory.get("summary"), dict) else 0)}</span>
          </div>
          <ul class="tight-list">{_list_items(flow_lessons.get("next_experiment", []) if isinstance(flow_lessons.get("next_experiment"), list) else [], 2)}</ul>
        </div>
      </div>
      <div class="panel-subhead"><h3>Flow Patterns</h3><span>{_esc((learning.get("prediction_accuracy") or {}).get("accuracy_percent", "unknown") if isinstance(learning.get("prediction_accuracy"), dict) else "unknown")}% prediction accuracy</span></div>
      <table><thead><tr><th>Flow</th><th class="right">Runs</th><th class="right">Avg Score</th><th>Best Mission</th></tr></thead><tbody>{_flow_rows(learning.get("flow_patterns", []) if isinstance(learning.get("flow_patterns"), list) else [])}</tbody></table>
      <div class="panel-subhead"><h3>Current Decisions</h3><span>from DECISION_JOURNAL.md</span></div>
      <table><thead><tr><th>Decision</th><th>Why</th><th>Revisit</th></tr></thead><tbody>{_decision_rows(memory_learning.get("decisions", []) if isinstance(memory_learning.get("decisions"), list) else [])}</tbody></table>
      <div class="panel-subhead"><h3>Recent Memory Documents</h3><span>top indexed items</span></div>
      <table><thead><tr><th>Document</th><th>Kind</th><th>Signals</th><th>Modified</th></tr></thead><tbody>{_document_rows(memory.get("recent_documents", []) if isinstance(memory.get("recent_documents"), list) else [])}</tbody></table>
    </section>

    <section class="panel" id="autonomy">
      <div class="panel-head"><h2>Autonomy Evidence</h2><span>{_esc(latest_run.get("path") or "No run folder")}</span></div>
      <div class="autonomy-grid">
        <div>
          <div class="eyebrow">Latest Run</div>
          <h3>{_esc(latest_run.get("mission_title") or latest_run.get("run_id") or "No autonomy run")}</h3>
          <p>{_esc(latest_run.get("last_summary") or latest_run.get("mission_goal") or "No run summary found.")}</p>
          <div class="run-strip">
            <span><strong>Status</strong> {_esc(latest_run.get("status") or "unknown")}</span>
            <span><strong>Phase</strong> {_esc(latest_run.get("phase") or "unknown")}</span>
            <span><strong>Updated</strong> {_esc(latest_run.get("updated_at") or "unknown")}</span>
          </div>
        </div>
        <div>
          <div class="eyebrow">Score</div>
          <div class="score-row">{_score_pills(latest_run.get("score", {}) if isinstance(latest_run.get("score"), dict) else {})}</div>
          <div class="eyebrow spacer">Next Action</div>
          <p>{_esc(latest_run.get("next_action") or "No next action recorded.")}</p>
        </div>
        <div>
          <div class="eyebrow">Latest Checkpoint</div>
          <h3>{_esc(latest_checkpoint.get("name") or "No checkpoint")}</h3>
          <p>{_esc(latest_checkpoint.get("summary") or "No checkpoint summary found.")}</p>
          <ul class="tight-list">{_list_items(latest_checkpoint.get("evidence", []) if isinstance(latest_checkpoint.get("evidence"), list) else [], 3)}</ul>
        </div>
      </div>
      <div class="panel-subhead"><h3>Recent Evidence</h3><span>{latest_run.get("evidence_count", 0)} evidence records</span></div>
      <table><thead><tr><th>Time</th><th>Phase</th><th>Evidence</th></tr></thead><tbody>{_evidence_rows(latest_run.get("evidence_tail", []) if isinstance(latest_run.get("evidence_tail"), list) else [])}</tbody></table>
      <div class="panel-subhead"><h3>Recent Checkpoints</h3><span>{latest_run.get("checkpoint_count", 0)} checkpoints</span></div>
      <table><thead><tr><th>Checkpoint</th><th>Phase</th><th>Status</th><th>Summary</th><th>Time</th></tr></thead><tbody>{_checkpoint_rows(latest_run.get("checkpoints", []) if isinstance(latest_run.get("checkpoints"), list) else [])}</tbody></table>
      <details class="resume-box"><summary>Resume Packet Preview</summary><pre>{_esc(latest_run.get("resume_preview") or "No resume packet found.")}</pre></details>
    </section>

    <section class="panel" id="agents">
      <div class="panel-head"><h2>Agent Queue / Handoff</h2><span>{_esc(agent_coordination.get("generated_at", ""))}</span></div>
      <div class="agent-grid">
        <div>
          <div class="eyebrow">Top Launchable Mission</div>
          <h3>{_esc(top_launch.get("title") or "No launch mission")}</h3>
          <p>{_esc(top_launch.get("why") or launch_queue.get("purpose") or "No launch queue found.")}</p>
          <div class="run-strip">
            <span><strong>Agent</strong> {_esc(top_launch.get("agent") or "unknown")}</span>
            <span><strong>Score</strong> {_esc(top_launch.get("rank_score") or 0)}</span>
            <span><strong>Timebox</strong> {_esc(top_launch.get("duration_minutes") or 0)}m</span>
          </div>
        </div>
        <div>
          <div class="eyebrow">Claim Slots</div>
          <div class="claims">{_claim_cards(claims)}</div>
        </div>
        <div>
          <div class="eyebrow">Safety</div>
          <ul class="tight-list">{_list_items(handoff_board.get("safety", []) if isinstance(handoff_board.get("safety"), list) else [], 5)}</ul>
        </div>
      </div>
      <div class="panel-subhead"><h3>Launch Queue</h3><span>{len(launch_missions)} ranked missions</span></div>
      <table><thead><tr><th>Mission</th><th>Agent</th><th>Status</th><th class="right">Score</th><th class="right">Timebox</th></tr></thead><tbody>{_launch_rows(launch_missions)}</tbody></table>
      <div class="panel-subhead"><h3>Open Handoffs</h3><span>{handoff_board.get("open_count", 0)} open / {handoff_board.get("claimed_count", 0)} claimed / {handoff_board.get("collision_count", 0)} collisions</span></div>
      <table><thead><tr><th>Mission</th><th>Agent</th><th>Status</th><th class="right">Priority</th><th>Write Scope</th></tr></thead><tbody>{_handoff_rows(handoff_missions)}</tbody></table>
    </section>

    <section class="panel" id="mission">
      <div class="panel-head"><h2>Mission Control</h2><span>{_esc(mission_control.get("generated_at", ""))}</span></div>
      <div class="mission-grid">
        <div>
          <div class="eyebrow">Current Mission</div>
          <h3>{_esc(mission.get("title") or "No active mission")}</h3>
          <p>{_esc(mission.get("active_summary") or active_autonomy.get("goal") or "No active mission summary found.")}</p>
          <div class="run-strip">
            <span><strong>Run</strong> {_esc(current_run.get("run_id") or "none")}</span>
            <span><strong>Updated</strong> {_esc(current_run.get("updated_at") or "unknown")}</span>
          </div>
        </div>
        <div>
          <div class="eyebrow">Next Agent</div>
          <h3>{_esc(next_agent.get("mission") or next_agent.get("title") or "No next-agent card")}</h3>
          <p>{_esc(next_agent.get("why") or "No why-this section found.")}</p>
          <ol class="tight-list">{_list_items(next_agent.get("start_here", []) if isinstance(next_agent.get("start_here"), list) else [], 4)}</ol>
        </div>
        <div>
          <div class="eyebrow">Stop Rules</div>
          <ul class="tight-list">{_list_items(next_agent.get("stop_rules", []) if isinstance(next_agent.get("stop_rules"), list) else mission.get("operating_rules", []), 5)}</ul>
        </div>
      </div>
      <div class="panel-subhead"><h3>Source Actions</h3><span>{len(source_cards)} cards ready for internal work</span></div>
      <table><thead><tr><th>Product</th><th>Source</th><th>Status</th><th>First Check / Issue</th></tr></thead><tbody>{_source_action_rows(source_cards)}</tbody></table>
    </section>

    <section class="panel">
      <div class="panel-head"><h2>Portfolio Health</h2><span>{_esc(portfolio.get("subtitle"))}</span></div>
      <div class="metrics compact">
        {_stat("Tracked", overview.get("Tracked", statuses["count"]))}
        {_stat("Green", overview.get("Green", health.get("green", 0)), "good")}
        {_stat("Yellow", overview.get("Yellow", health.get("yellow", 0)), "warn")}
        {_stat("Red", overview.get("Red", health.get("red", 0)), "bad")}
        {_stat("Unknown", overview.get("Unknown", health.get("unknown", 0)))}
      </div>
    </section>

    <section class="grid-two">
      <div class="panel" id="briefing">
        <div class="panel-head"><h2>Morning Briefing</h2><span>{_esc(artifacts["morning"]["updated_at"] or "missing")}</span></div>
        <pre class="brief">{_esc(state["morning"]["preview"] or "No morning brief found.")}</pre>
      </div>
      <div class="panel" id="costs">
        <div class="panel-head"><h2>Cost Rollup</h2><span>{_esc(artifacts["portfolio_cost"]["updated_at"] or "missing")}</span></div>
        <table><thead><tr><th>Recipe</th><th class="right">30d Cost</th></tr></thead><tbody>{_cost_rows(cost)}</tbody></table>
      </div>
    </section>

    <section class="panel" id="review">
      <div class="panel-head"><h2>Weekly Review</h2><span>{auto_count} auto-merged / {reviewed_count} reviewed</span></div>
      <table><thead><tr><th>Repo</th><th>PR</th><th>Title</th><th class="right">Size</th><th class="right">Files</th></tr></thead><tbody>{_weekly_rows(weekly)}</tbody></table>
    </section>

    <section class="panel" id="sources">
      <div class="panel-head"><h2>War-room Source Registry</h2><span>{len(missing_connections)} existing source groups still not connected to cockpit</span></div>
      <div class="metrics compact">
        {_stat("Sources", registry_summary.get("total", 0))}
        {_stat("Connected", registry_summary.get("connected", 0), "good")}
        {_stat("Static Only", registry_summary.get("static_only", 0), "warn")}
        {_stat("Not Connected", registry_summary.get("not_connected", 0), "warn")}
        {_stat("Missing", registry_summary.get("missing", 0), "bad" if registry_summary.get("missing", 0) else "")}
      </div>
      <table class="source-table"><thead><tr><th>Connection</th><th>Source</th><th>Category</th><th>Target</th><th>Health</th><th class="right">Files</th><th>Latest</th></tr></thead><tbody>{_source_rows(registry.get("items", []) if isinstance(registry.get("items"), list) else [])}</tbody></table>
    </section>

    <section class="panel" id="status">
      <div class="panel-head"><h2>Status Stream</h2><span>{_esc(state["paths"]["status_dir"])}</span></div>
      <table><thead><tr><th>Project</th><th>Health</th><th>Summary</th><th>Timestamp</th></tr></thead><tbody>{_status_rows(statuses["items"])}</tbody></table>
    </section>
  </main>
</div>
<script>
async function refreshCockpit() {{
  try {{
    const resp = await fetch('/cockpit.json', {{cache: 'no-store'}});
    if (!resp.ok) return;
    const data = await resp.json();
    const el = document.getElementById('last-refresh');
    if (el) el.textContent = data.generated_at;
  }} catch (e) {{}}
}}
setInterval(refreshCockpit, 15000);
</script>
</body>
</html>"""


def register_cockpit_routes() -> None:
    def _get_cockpit(handler: Any, body: Any) -> tuple[int, str]:
        return 200, render_cockpit(collect_cockpit_state())

    def _get_cockpit_json(handler: Any, body: Any) -> tuple[int, dict[str, Any]]:
        return 200, collect_cockpit_state()

    register_extra_route("GET", "/cockpit", _get_cockpit)
    register_extra_route("GET", "/cockpit.json", _get_cockpit_json)


_CSS = """
:root {
  --bg: #07090d;
  --panel: #10141b;
  --panel-2: #151a23;
  --line: #27303d;
  --text: #edf1f7;
  --muted: #929baa;
  --good: #7ac694;
  --warn: #d8aa55;
  --bad: #e36c6c;
  --accent: #6ea8ff;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.45 "Segoe UI", Arial, sans-serif; }
.mono { font-family: Consolas, "SFMono-Regular", monospace; }
.shell { min-height: 100vh; display: grid; grid-template-columns: 210px minmax(0, 1fr); }
.rail { border-right: 1px solid var(--line); padding: 18px 14px; background: #0b0e14; position: sticky; top: 0; height: 100vh; }
.brand { font-weight: 700; letter-spacing: 0.08em; margin-bottom: 24px; }
.brand span { color: var(--accent); }
.rail a { display: block; color: var(--muted); text-decoration: none; padding: 8px 6px; border-left: 2px solid transparent; }
.rail a:hover { color: var(--text); border-left-color: var(--accent); background: var(--panel); }
main { padding: 20px; max-width: 1500px; width: 100%; }
.topbar { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; margin-bottom: 18px; }
h1 { margin: 0; font-size: 28px; }
h2 { margin: 0; font-size: 16px; }
h3 { margin: 0 0 6px; font-size: 17px; }
p { margin: 4px 0 0; }
.muted, .panel-head span { color: var(--muted); }
.links { display: flex; gap: 8px; flex-wrap: wrap; }
.links a { color: var(--text); border: 1px solid var(--line); padding: 6px 10px; text-decoration: none; background: var(--panel); }
.metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin-bottom: 14px; }
.metrics.compact { grid-template-columns: repeat(5, minmax(110px, 1fr)); margin: 0; }
.metric { border: 1px solid var(--line); background: var(--panel); padding: 10px; min-height: 72px; }
.metric-label { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; }
.metric-value { font-size: 24px; margin-top: 6px; font-weight: 650; }
.metric.good .metric-value { color: var(--good); }
.metric.warn .metric-value { color: var(--warn); }
.metric.bad .metric-value { color: var(--bad); }
.panel { border: 1px solid var(--line); background: var(--panel); padding: 14px; margin-bottom: 14px; }
.panel-head { display: flex; justify-content: space-between; gap: 12px; align-items: baseline; margin-bottom: 12px; }
.panel-subhead { display: flex; justify-content: space-between; gap: 12px; align-items: baseline; margin: 16px 0 8px; }
.panel-subhead span { color: var(--muted); }
.grid-two { display: grid; grid-template-columns: minmax(0, 1.15fr) minmax(0, 0.85fr); gap: 14px; }
.mission-grid, .agent-grid, .autonomy-grid, .memory-grid { display: grid; grid-template-columns: minmax(0, 1.25fr) minmax(0, 1fr) minmax(260px, 0.8fr); gap: 14px; }
.mission-grid > div, .agent-grid > div, .autonomy-grid > div, .memory-grid > div { border: 1px solid var(--line); background: var(--panel-2); padding: 12px; min-height: 170px; }
.eyebrow { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px; }
.spacer { margin-top: 14px; }
.run-strip { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 12px; color: var(--muted); font-size: 12px; }
.run-strip strong { color: var(--text); }
.tight-list { margin: 8px 0 0; padding-left: 20px; color: var(--text); }
.tight-list li { margin-bottom: 5px; }
.empty-inline { color: var(--muted); list-style: none; margin-left: -20px; }
.claims { display: grid; gap: 8px; }
.claim { border: 1px solid var(--line); background: var(--panel); padding: 8px; }
.claim.free { border-color: rgba(122, 198, 148, 0.35); }
.claim.claimed, .claim.busy { border-color: rgba(216, 170, 85, 0.45); }
.claim-agent { font-weight: 700; }
.claim-status { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; }
.score-row { display: flex; flex-wrap: wrap; gap: 8px; }
.score-pill { display: inline-block; border: 1px solid rgba(110, 168, 255, 0.45); color: var(--accent); background: rgba(110, 168, 255, 0.08); padding: 4px 8px; font-size: 12px; }
.resume-box { margin-top: 12px; border: 1px solid var(--line); background: var(--panel-2); padding: 10px; }
.resume-box summary { cursor: pointer; color: var(--text); }
.resume-box pre { white-space: pre-wrap; color: var(--muted); max-height: 360px; overflow: auto; }
table { width: 100%; border-collapse: collapse; }
th, td { border-bottom: 1px solid var(--line); padding: 7px 8px; text-align: left; vertical-align: top; }
th { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; }
.right { text-align: right; }
.empty { color: var(--muted); text-align: center; padding: 24px; }
.subtle { color: var(--muted); font-size: 12px; margin-top: 2px; }
.small { font-size: 12px; }
.source-table { margin-top: 12px; }
.badge, .health { display: inline-block; border: 1px solid var(--line); padding: 2px 7px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; }
.badge.connected, .health.ok { color: var(--good); border-color: rgba(122, 198, 148, 0.45); background: rgba(122, 198, 148, 0.08); }
.badge.static-only, .badge.not-connected, .health.stale { color: var(--warn); border-color: rgba(216, 170, 85, 0.45); background: rgba(216, 170, 85, 0.08); }
.health.missing { color: var(--bad); border-color: rgba(227, 108, 108, 0.45); background: rgba(227, 108, 108, 0.08); }
.brief { white-space: pre-wrap; margin: 0; min-height: 220px; color: var(--text); background: var(--panel-2); border: 1px solid var(--line); padding: 10px; overflow: auto; }
.dot { display: inline-block; width: 7px; height: 7px; margin-right: 8px; border-radius: 50%; background: var(--muted); }
.dot.green { background: var(--good); }
.dot.yellow { background: var(--warn); }
.dot.red { background: var(--bad); }
@media (max-width: 980px) {
  .shell { grid-template-columns: 1fr; }
  .rail { position: static; height: auto; border-right: 0; border-bottom: 1px solid var(--line); }
  .rail a { display: inline-block; }
  .metrics, .metrics.compact, .grid-two, .mission-grid, .agent-grid, .autonomy-grid, .memory-grid { grid-template-columns: 1fr 1fr; }
}
@media (max-width: 620px) {
  main { padding: 12px; }
  .metrics, .metrics.compact, .grid-two, .mission-grid, .agent-grid, .autonomy-grid, .memory-grid { grid-template-columns: 1fr; }
  .topbar, .panel-head { display: block; }
}
"""
