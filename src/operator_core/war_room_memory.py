"""Read-only Memory OS and Learning Loop state from war-room artifacts."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def collect_memory_learning(war_room_dir: Path) -> dict[str, Any]:
    memory_path = war_room_dir / "memory-os.json"
    learning_path = war_room_dir / "learning-loop.json"
    flow_recommendation_path = war_room_dir / "FLOW_RECOMMENDATION.md"
    flow_lessons_path = war_room_dir / "FLOW_LESSONS.md"
    flow_library_path = war_room_dir / "FLOW_LIBRARY.md"
    decision_journal_path = war_room_dir / "DECISION_JOURNAL.md"
    scoreboard_path = war_room_dir / "real-agent-scoreboard.json"

    memory = _read_json(memory_path, {})
    learning = _read_json(learning_path, {})
    scoreboard = _read_json(scoreboard_path, {})

    flow_recommendation_text = _read_text(flow_recommendation_path)
    flow_lessons_text = _read_text(flow_lessons_path)
    flow_library_text = _read_text(flow_library_path)
    decision_text = _read_text(decision_journal_path)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "memory": _memory_summary(memory if isinstance(memory, dict) else {}),
        "learning": _learning_summary(learning if isinstance(learning, dict) else {}),
        "flow_recommendation": _flow_recommendation(flow_recommendation_text),
        "flow_lessons": {
            "learned": _bullet_section(flow_lessons_text, "What The War Room Has Learned"),
            "best_observed": _bullet_section(flow_lessons_text, "Best Observed Flow"),
            "weakest_observed": _bullet_section(flow_lessons_text, "Weakest Observed Flow"),
            "next_experiment": _bullet_section(flow_lessons_text, "Next Experiment"),
        },
        "flow_library": {
            "scoreboard": _bullet_section(flow_library_text, "Flow Scoreboard"),
        },
        "decisions": _decision_entries(decision_text),
        "scoreboard": _scoreboard_summary(scoreboard if isinstance(scoreboard, dict) else {}),
        "artifacts": {
            "memory_os": _meta(memory_path),
            "learning_loop": _meta(learning_path),
            "flow_recommendation": _meta(flow_recommendation_path),
            "flow_lessons": _meta(flow_lessons_path),
            "flow_library": _meta(flow_library_path),
            "decision_journal": _meta(decision_journal_path),
            "real_agent_scoreboard": _meta(scoreboard_path),
        },
    }


def _memory_summary(doc: dict[str, Any]) -> dict[str, Any]:
    summary = doc.get("summary") if isinstance(doc.get("summary"), dict) else {}
    pillars = doc.get("pillars") if isinstance(doc.get("pillars"), list) else []
    documents = doc.get("documents") if isinstance(doc.get("documents"), list) else []
    decisions = doc.get("decisions") if isinstance(doc.get("decisions"), list) else []
    timeline = doc.get("timeline") if isinstance(doc.get("timeline"), list) else []
    return {
        "version": str(doc.get("version") or ""),
        "generated_at": str(doc.get("generated_at") or ""),
        "summary": summary,
        "pillars": [_pick(p, "id", "name", "role", "purpose") for p in pillars[:8] if isinstance(p, dict)],
        "recent_documents": [
            _pick(item, "title", "kind", "relative_path", "mtime", "signals")
            for item in documents[:12]
            if isinstance(item, dict)
        ],
        "decisions": [
            _pick(item, "title", "preview", "source", "mtime")
            for item in decisions[:8]
            if isinstance(item, dict)
        ],
        "timeline": timeline[:12],
        "ask_workspace_prompts": doc.get("ask_workspace_prompts") if isinstance(doc.get("ask_workspace_prompts"), list) else [],
    }


def _learning_summary(doc: dict[str, Any]) -> dict[str, Any]:
    latest_run = doc.get("latest_run") if isinstance(doc.get("latest_run"), dict) else {}
    pattern_detector = doc.get("pattern_detector") if isinstance(doc.get("pattern_detector"), dict) else {}
    prediction_accuracy = doc.get("prediction_accuracy") if isinstance(doc.get("prediction_accuracy"), dict) else {}
    flow_patterns = doc.get("flow_patterns") if isinstance(doc.get("flow_patterns"), list) else []
    replays = doc.get("replays") if isinstance(doc.get("replays"), list) else []
    return {
        "mode": str(doc.get("mode") or ""),
        "purpose": str(doc.get("purpose") or ""),
        "run_count": int(doc.get("run_count") or len(replays)),
        "replay_count": int(doc.get("replay_count") or len(replays)),
        "latest_run": _pick(
            latest_run,
            "id",
            "agent",
            "project",
            "mission",
            "flow",
            "grade",
            "duration_minutes",
            "universal_score",
            "artifact_count",
            "verification_count",
            "verdict",
            "next_sprint",
            "repeat_signal",
            "drag_signal",
        ),
        "flow_patterns": [
            _pick(item, "flow", "run_count", "average_score", "average_duration_minutes", "best_mission", "best_grade")
            for item in flow_patterns[:8]
            if isinstance(item, dict)
        ],
        "pattern_detector": {
            "fastest_flow": _pick(pattern_detector.get("fastest_flow") if isinstance(pattern_detector.get("fastest_flow"), dict) else {}, "flow", "average_score", "average_duration_minutes", "best_mission"),
            "highest_grade_flow": _pick(pattern_detector.get("highest_grade_flow") if isinstance(pattern_detector.get("highest_grade_flow"), dict) else {}, "flow", "average_score", "average_duration_minutes", "best_mission"),
            "safest_flow": _pick(pattern_detector.get("safest_flow") if isinstance(pattern_detector.get("safest_flow"), dict) else {}, "flow", "average_score", "average_duration_minutes", "best_mission"),
        },
        "prediction_accuracy": _pick(prediction_accuracy, "known", "matches", "misses", "accuracy_percent"),
        "next_learning_sprint": str(doc.get("next_learning_sprint") or ""),
        "safety": doc.get("safety") if isinstance(doc.get("safety"), list) else [],
    }


def _scoreboard_summary(doc: dict[str, Any]) -> dict[str, Any]:
    real_runs = doc.get("real_runs") if isinstance(doc.get("real_runs"), list) else []
    return {
        "date": str(doc.get("date") or ""),
        "real_runs": [
            _pick(item, "agent", "workflow", "mission", "total", "lesson", "source_run")
            for item in real_runs[:10]
            if isinstance(item, dict)
        ],
    }


def _flow_recommendation(text: str) -> dict[str, Any]:
    bullets = _bullet_section(text, "Recommended Flow")
    parsed: dict[str, str] = {}
    for item in bullets:
        key, sep, value = item.partition(":")
        if sep:
            parsed[_norm(key)] = _strip_md(value)
    return {
        "mission": parsed.get("mission", ""),
        "flow": parsed.get("flow", parsed.get("recommended_flow", "")),
        "confidence": parsed.get("confidence", ""),
        "historical_runs": parsed.get("historical_runs_for_this_flow", ""),
        "average_flow_score": parsed.get("average_flow_score", ""),
        "why": _bullet_section(text, "Why This Flow"),
        "run_contract": _bullet_section(text, "Run Contract"),
        "learning_rule": _bullet_section(text, "Learning Rule"),
    }


def _decision_entries(text: str) -> list[dict[str, str]]:
    section = _section_text(text, "Current System Decisions")
    entries: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in section.splitlines():
        match = re.match(r"^\s*-\s+([^:]+):\s*(.*)$", line)
        if not match:
            continue
        key = _norm(match.group(1))
        value = _strip_md(match.group(2))
        if key == "decision" and current:
            entries.append(current)
            current = {}
        current[key] = value
    if current:
        entries.append(current)
    return entries[:8]


def _bullet_section(text: str, heading: str) -> list[str]:
    section = _section_text(text, heading)
    bullets = []
    for line in section.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            bullets.append(_strip_md(stripped[2:]))
    return bullets


def _section_text(text: str, heading: str) -> str:
    wanted = _norm(heading)
    out = []
    capture = False
    for line in text.splitlines():
        if line.startswith("## "):
            if capture:
                break
            capture = _norm(line[3:]) == wanted
            continue
        if capture:
            out.append(line)
    return "\n".join(out).strip()


def _strip_md(text: str) -> str:
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    return text.replace("`", "").strip()


def _pick(doc: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: doc.get(key) for key in keys if key in doc}


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


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
