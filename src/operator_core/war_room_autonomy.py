"""Read-only autonomy run evidence from war-room artifacts."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def collect_autonomy_evidence(war_room_dir: Path) -> dict[str, Any]:
    autonomy_dir = war_room_dir / "autonomy"
    runs_dir = autonomy_dir / "runs"
    run_index_path = autonomy_dir / "run-index.jsonl"

    run_index = _read_jsonl(run_index_path)
    latest_entry = _latest_run_entry(run_index, runs_dir)
    latest_run_dir = _resolve_run_dir(latest_entry, runs_dir)
    latest_run = _collect_run(latest_run_dir) if latest_run_dir else {}

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_count": len(run_index),
        "run_index": run_index[-20:],
        "latest": latest_run,
        "artifacts": {
            "run_index": _meta(run_index_path),
            "runs_dir": _meta(runs_dir),
            "latest_run_dir": _meta(latest_run_dir) if latest_run_dir else {"exists": False, "path": "", "updated_at": None, "size": 0},
        },
    }


def _collect_run(run_dir: Path) -> dict[str, Any]:
    status_path = run_dir / "status.json"
    mission_path = run_dir / "mission_snapshot.json"
    resume_path = run_dir / "resume.md"
    handoff_path = run_dir / "handoff_prompt.md"
    evidence_path = run_dir / "evidence.jsonl"

    status = _read_json(status_path, {})
    mission_snapshot = _read_json(mission_path, {})
    evidence = _read_jsonl(evidence_path)
    checkpoints = _collect_checkpoints(run_dir)
    latest_checkpoint = checkpoints[-1] if checkpoints else {}
    mission = mission_snapshot.get("mission") if isinstance(mission_snapshot, dict) and isinstance(mission_snapshot.get("mission"), dict) else {}

    return {
        "run_id": str(status.get("run_id") or run_dir.name) if isinstance(status, dict) else run_dir.name,
        "mission_id": str(status.get("mission_id") or mission.get("id") or "") if isinstance(status, dict) else "",
        "mission_title": str(mission.get("title") or ""),
        "status": str(status.get("status") or "") if isinstance(status, dict) else "",
        "phase": str(status.get("phase") or "") if isinstance(status, dict) else "",
        "started_at": str(status.get("started_at") or "") if isinstance(status, dict) else "",
        "updated_at": str(status.get("updated_at") or "") if isinstance(status, dict) else "",
        "horizon_hours": status.get("horizon_hours") if isinstance(status, dict) else None,
        "checkpoint_minutes": status.get("checkpoint_minutes") if isinstance(status, dict) else None,
        "last_summary": str(status.get("last_summary") or "") if isinstance(status, dict) else "",
        "next_action": str(status.get("next_action") or "") if isinstance(status, dict) else "",
        "blocker": str(status.get("blocker") or "") if isinstance(status, dict) else "",
        "score": status.get("score") if isinstance(status, dict) and isinstance(status.get("score"), dict) else {},
        "path": str(run_dir),
        "checkpoint_count": len(checkpoints),
        "latest_checkpoint": latest_checkpoint,
        "checkpoints": checkpoints[-8:],
        "evidence_count": len(evidence),
        "evidence_tail": evidence[-10:],
        "resume_preview": "\n".join(_read_text(resume_path).splitlines()[:45]),
        "handoff_preview": "\n".join(_read_text(handoff_path).splitlines()[:45]),
        "mission_goal": str(mission.get("goal") or ""),
        "stop_conditions": mission.get("stop_conditions") if isinstance(mission.get("stop_conditions"), list) else [],
        "verification_gates": mission.get("verification_gates") if isinstance(mission.get("verification_gates"), list) else [],
        "artifacts": {
            "status": _meta(status_path),
            "mission_snapshot": _meta(mission_path),
            "resume": _meta(resume_path),
            "handoff_prompt": _meta(handoff_path),
            "evidence": _meta(evidence_path),
        },
    }


def _collect_checkpoints(run_dir: Path) -> list[dict[str, Any]]:
    checkpoints = []
    for path in sorted(run_dir.glob("checkpoint_*.md")):
        text = _read_text(path)
        meta = _checkpoint_meta(text)
        meta.update(_meta(path))
        meta["name"] = path.name
        meta["summary"] = _section_text(text, "Summary")
        meta["next"] = _section_text(text, "Next")
        meta["evidence"] = _bullet_section(text, "Evidence")
        checkpoints.append(meta)
    return checkpoints


def _checkpoint_meta(text: str) -> dict[str, Any]:
    out = {"time": "", "phase": "", "status": ""}
    for line in text.splitlines():
        match = re.match(r"^\s*-\s+(Time|Phase|Status):\s*(.*)$", line, re.IGNORECASE)
        if match:
            out[match.group(1).lower()] = match.group(2).strip()
    return out


def _section_text(text: str, heading: str) -> str:
    wanted = heading.lower()
    lines = text.splitlines()
    capturing = False
    out = []
    for line in lines:
        if line.startswith("## "):
            if capturing:
                break
            capturing = line[3:].strip().lower() == wanted
            continue
        if capturing:
            out.append(line)
    return "\n".join(out).strip()


def _bullet_section(text: str, heading: str) -> list[str]:
    section = _section_text(text, heading)
    bullets = []
    for line in section.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            bullets.append(stripped[2:].strip())
    return bullets


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default
    except (OSError, json.JSONDecodeError):
        return default


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return rows
    for line in lines:
        if not line.strip():
            continue
        try:
            value = json.loads(line)
            rows.append(value if isinstance(value, dict) else {"value": value})
        except json.JSONDecodeError:
            rows.append({"raw": line})
    return rows


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
    except OSError:
        return ""


def _latest_run_entry(run_index: list[dict[str, Any]], runs_dir: Path) -> dict[str, Any]:
    if run_index:
        return sorted(run_index, key=lambda item: str(item.get("updated_at") or item.get("run_id") or ""))[-1]
    if not runs_dir.exists():
        return {}
    dirs = [p for p in runs_dir.iterdir() if p.is_dir()]
    if not dirs:
        return {}
    latest = max(dirs, key=lambda path: path.stat().st_mtime)
    return {"run_id": latest.name, "path": str(latest)}


def _resolve_run_dir(entry: dict[str, Any], runs_dir: Path) -> Path | None:
    path_value = entry.get("path") if isinstance(entry, dict) else ""
    if path_value:
        path = Path(str(path_value))
        if path.exists() and path.is_dir():
            return path
    run_id = entry.get("run_id") if isinstance(entry, dict) else ""
    if run_id:
        path = runs_dir / str(run_id)
        if path.exists() and path.is_dir():
            return path
    return None


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
