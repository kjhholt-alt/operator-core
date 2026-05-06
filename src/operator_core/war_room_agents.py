"""Read-only agent queue and handoff state from war-room artifacts."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def collect_agent_coordination(war_room_dir: Path) -> dict[str, Any]:
    launch_path = war_room_dir / "agent-launch-queue.json"
    handoff_path = war_room_dir / "agent-handoff-board.json"
    open_queue_path = war_room_dir / "handoffs" / "OPEN_QUEUE.md"
    side_open_queue_path = war_room_dir / "handoffs" / "side-projects" / "OPEN_QUEUE.md"
    claim_paths = sorted((war_room_dir / "handoffs").glob("*.md")) if (war_room_dir / "handoffs").exists() else []

    launch_doc = _read_json(launch_path, {})
    handoff_doc = _read_json(handoff_path, {})
    launch_missions = _normalize_launch_missions(launch_doc if isinstance(launch_doc, dict) else {})
    handoff_missions = _normalize_handoff_missions(handoff_doc if isinstance(handoff_doc, dict) else {})
    claims = _normalize_claims(
        handoff_doc.get("claims") if isinstance(handoff_doc, dict) and isinstance(handoff_doc.get("claims"), list) else [],
        claim_paths,
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "launch_queue": {
            "mode": str(launch_doc.get("mode") or "") if isinstance(launch_doc, dict) else "",
            "purpose": str(launch_doc.get("purpose") or "") if isinstance(launch_doc, dict) else "",
            "mission_count": len(launch_missions),
            "top_mission": launch_missions[0] if launch_missions else {},
            "missions": launch_missions,
            "safety": launch_doc.get("safety") if isinstance(launch_doc.get("safety"), list) else [],
        },
        "handoff_board": {
            "mode": str(handoff_doc.get("mode") or "") if isinstance(handoff_doc, dict) else "",
            "purpose": str(handoff_doc.get("purpose") or "") if isinstance(handoff_doc, dict) else "",
            "open_count": int(handoff_doc.get("open_count") or len([m for m in handoff_missions if m.get("status") == "open"])) if isinstance(handoff_doc, dict) else 0,
            "claimed_count": int(handoff_doc.get("claimed_count") or 0) if isinstance(handoff_doc, dict) else 0,
            "collision_count": int(handoff_doc.get("collision_count") or 0) if isinstance(handoff_doc, dict) else 0,
            "missions": handoff_missions,
            "claims": claims,
            "collisions": handoff_doc.get("collisions") if isinstance(handoff_doc.get("collisions"), list) else [],
            "safety": handoff_doc.get("safety") if isinstance(handoff_doc.get("safety"), list) else [],
        },
        "open_queue": {
            "path": str(open_queue_path),
            "preview": "\n".join(_read_text(open_queue_path).splitlines()[:80]),
        },
        "side_project_queue": {
            "path": str(side_open_queue_path),
            "exists": side_open_queue_path.exists(),
            "preview": "\n".join(_read_text(side_open_queue_path).splitlines()[:50]),
        },
        "artifacts": {
            "launch_queue": _meta(launch_path),
            "handoff_board": _meta(handoff_path),
            "open_queue": _meta(open_queue_path),
            "side_project_open_queue": _meta(side_open_queue_path),
            "claim_files": [_meta(path) for path in claim_paths if _is_claim_file(path)],
        },
    }


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default
    except (OSError, json.JSONDecodeError):
        return default


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
    except OSError:
        return ""


def _meta(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
        return {
            "path": str(path),
            "exists": True,
            "updated_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            "size": stat.st_size,
        }
    except OSError:
        return {"path": str(path), "exists": False, "updated_at": None, "size": 0}


def _normalize_launch_missions(doc: dict[str, Any]) -> list[dict[str, Any]]:
    missions = doc.get("missions") if isinstance(doc.get("missions"), list) else []
    out = []
    for raw in missions:
        if not isinstance(raw, dict):
            continue
        out.append({
            "id": str(raw.get("id") or ""),
            "title": str(raw.get("title") or ""),
            "agent": str(raw.get("best_agent") or raw.get("recommended_agent") or ""),
            "status": str(raw.get("status") or ""),
            "energy_mode": str(raw.get("energy_mode") or ""),
            "duration_minutes": _int(raw.get("duration_minutes")),
            "rank_score": _int(raw.get("rank_score")),
            "autonomy_score": _int(raw.get("autonomy_score")),
            "why": str(raw.get("why") or ""),
            "output": str(raw.get("output_artifact") or raw.get("output") or ""),
            "resume_command": str(raw.get("resume_command") or ""),
            "stop_rule": str(raw.get("stop_rule") or ""),
            "verification": raw.get("verification") if isinstance(raw.get("verification"), list) else [],
        })
    return sorted(out, key=lambda item: item.get("rank_score") or 0, reverse=True)


def _normalize_handoff_missions(doc: dict[str, Any]) -> list[dict[str, Any]]:
    missions = doc.get("missions") if isinstance(doc.get("missions"), list) else []
    out = []
    for raw in missions:
        if not isinstance(raw, dict):
            continue
        out.append({
            "id": str(raw.get("id") or ""),
            "title": str(raw.get("title") or ""),
            "agent": str(raw.get("recommended_agent") or raw.get("best_agent") or ""),
            "status": str(raw.get("status") or ""),
            "priority": _int(raw.get("priority")),
            "estimated_minutes": _int(raw.get("estimated_minutes") or raw.get("duration_minutes")),
            "source": str(raw.get("source") or ""),
            "why": str(raw.get("why") or ""),
            "output": str(raw.get("output") or raw.get("output_artifact") or ""),
            "claim_file": str(raw.get("claim_file") or ""),
            "write_scope": raw.get("write_scope") if isinstance(raw.get("write_scope"), list) else [],
            "read_first": raw.get("read_first") if isinstance(raw.get("read_first"), list) else [],
            "verification": raw.get("verification") if isinstance(raw.get("verification"), list) else [],
            "stop_rule": str(raw.get("stop_rule") or ""),
        })
    return sorted(out, key=lambda item: item.get("priority") or 0, reverse=True)


def _normalize_claims(json_claims: list[Any], claim_paths: list[Path]) -> list[dict[str, Any]]:
    by_agent: dict[str, dict[str, Any]] = {}
    for claim in json_claims:
        if not isinstance(claim, dict):
            continue
        agent = str(claim.get("agent") or "").upper()
        if not agent:
            continue
        by_agent[agent] = {
            "agent": agent,
            "path": str(claim.get("path") or ""),
            "status": str(claim.get("status") or ""),
            "mission_id": str(claim.get("mission_id") or ""),
            "started": str(claim.get("started") or ""),
            "write_scope": "",
            "stop_rule": "",
            "notes": str(claim.get("notes") or ""),
        }
    for path in claim_paths:
        agent = path.stem.upper()
        if not _is_claim_file(path):
            continue
        parsed = _parse_claim_file(path)
        base = by_agent.get(agent, {"agent": agent, "path": str(path)})
        base.update({k: v for k, v in parsed.items() if v or k not in base})
        base["path"] = str(path)
        by_agent[agent] = base
    return [by_agent[key] for key in sorted(by_agent)]


def _is_claim_file(path: Path) -> bool:
    agent = path.stem.upper()
    return agent not in {"README", "OPEN_QUEUE", "CLAIM_TEMPLATE"} and not agent.startswith("_")


def _parse_claim_file(path: Path) -> dict[str, Any]:
    text = _read_text(path)
    fields: dict[str, str] = {}
    notes: list[str] = []
    in_notes = False
    for line in text.splitlines():
        if line.strip().lower() == "## notes":
            in_notes = True
            continue
        if in_notes:
            notes.append(line)
        match = re.match(r"^\s*-\s+([^:]+):\s*(.*)$", line)
        if match:
            fields[_norm(match.group(1))] = match.group(2).strip()
    return {
        "status": fields.get("status", ""),
        "mission_id": fields.get("mission_id", ""),
        "agent": fields.get("agent", path.stem.upper()).upper(),
        "started": fields.get("started", ""),
        "write_scope": fields.get("write_scope", ""),
        "stop_rule": fields.get("stop_rule", ""),
        "notes": "\n".join(notes).strip(),
    }


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
