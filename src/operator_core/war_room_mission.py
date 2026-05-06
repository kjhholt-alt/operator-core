"""Read-only Mission Control state from war-room artifacts."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def collect_mission_control(war_room_dir: Path) -> dict[str, Any]:
    active_mission_path = war_room_dir / "ACTIVE_MISSION.md"
    current_run_path = war_room_dir / "current-run.json"
    next_agent_path = war_room_dir / "NEXT_AGENT_CARD.md"
    source_actions_path = war_room_dir / "SOURCE_ACTIONS.md"
    autonomy_active_path = war_room_dir / "autonomy" / "missions" / "active.json"

    active_text = _read_text(active_mission_path)
    next_agent_text = _read_text(next_agent_path)
    source_actions_text = _read_text(source_actions_path)
    current_run = _read_json(current_run_path, {})
    autonomy_active = _read_json(autonomy_active_path, {})

    active_sections = _markdown_sections(active_text)
    next_sections = _markdown_sections(next_agent_text)
    source_cards = _source_action_cards(source_actions_text)
    active_autonomy = _active_autonomy_summary(autonomy_active if isinstance(autonomy_active, dict) else {})

    next_agent = {
        "title": _markdown_title(next_agent_text) or "Next Agent Card",
        "mission": _section_text(next_sections, "Mission"),
        "why": _section_text(next_sections, "Why This"),
        "start_here": _ordered_list(_section_text(next_sections, "Start Here")),
        "verify": _section_text(next_sections, "Verify"),
        "stop_rules": _bullet_list(_section_text(next_sections, "Stop Rules")),
        "record_the_run": _section_text(next_sections, "Record The Run"),
        "preview": "\n".join(next_agent_text.splitlines()[:30]),
    }

    mission_title = (
        next_agent["mission"]
        or active_autonomy.get("title")
        or _first_paragraph(_section_text(active_sections, "Mission"))
        or _markdown_title(active_text)
        or "No active mission"
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mission": {
            "title": mission_title,
            "active_summary": _first_paragraph(_section_text(active_sections, "Mission")),
            "current_product_lane": _section_text(active_sections, "Current Best Product Lane"),
            "current_meta_lane": _section_text(active_sections, "Current Meta-Lane"),
            "active_marathon_sprint": _section_text(active_sections, "Active Marathon Sprint"),
            "operating_rules": _bullet_list(_section_text(active_sections, "Operating Rules")),
            "raw_preview": "\n".join(active_text.splitlines()[:32]),
        },
        "current_run": _current_run_summary(current_run if isinstance(current_run, dict) else {}),
        "active_autonomy": active_autonomy,
        "next_agent": next_agent,
        "source_actions": {
            "count": len(source_cards),
            "cards": source_cards,
            "preview": "\n".join(source_actions_text.splitlines()[:40]),
        },
        "artifacts": {
            "active_mission": _meta(active_mission_path),
            "current_run": _meta(current_run_path),
            "next_agent_card": _meta(next_agent_path),
            "source_actions": _meta(source_actions_path),
            "autonomy_active": _meta(autonomy_active_path),
        },
    }


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
    except OSError:
        return ""


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default
    except (OSError, json.JSONDecodeError):
        return default


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


def _markdown_title(text: str) -> str:
    for line in text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return ""


def _markdown_sections(text: str) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current = ""
    for line in text.splitlines():
        if line.startswith("## "):
            current = line[3:].strip()
            sections.setdefault(current, [])
            continue
        if current:
            sections[current].append(line)
    return {key: "\n".join(value).strip() for key, value in sections.items()}


def _section_text(sections: dict[str, str], heading: str) -> str:
    wanted = _norm_heading(heading)
    for key, value in sections.items():
        if _norm_heading(key) == wanted:
            return value.strip()
    return ""


def _norm_heading(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _first_paragraph(text: str) -> str:
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            if lines:
                break
            continue
        if stripped.startswith(("-", "*", "#")):
            continue
        lines.append(stripped)
    return " ".join(lines)


def _bullet_list(text: str) -> list[str]:
    out = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            out.append(_strip_md(stripped[2:]))
    return out


def _ordered_list(text: str) -> list[str]:
    out = []
    for line in text.splitlines():
        match = re.match(r"^\s*\d+\.\s+(.*)$", line)
        if match:
            out.append(_strip_md(match.group(1)))
    return out


def _strip_md(text: str) -> str:
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = text.replace("`", "")
    return text.strip()


def _current_run_summary(doc: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": str(doc.get("run_id") or ""),
        "mission_id": str(doc.get("mission_id") or ""),
        "updated_at": str(doc.get("updated_at") or ""),
        "path": str(doc.get("path") or ""),
    }


def _active_autonomy_summary(doc: dict[str, Any]) -> dict[str, Any]:
    mission = doc.get("mission") if isinstance(doc.get("mission"), dict) else {}
    return {
        "id": str(mission.get("id") or ""),
        "title": str(mission.get("title") or ""),
        "mode": str(mission.get("mode") or ""),
        "horizon_hours": mission.get("horizon_hours"),
        "checkpoint_minutes": mission.get("checkpoint_minutes"),
        "goal": str(mission.get("goal") or ""),
        "success_criteria": mission.get("success_criteria") if isinstance(mission.get("success_criteria"), list) else [],
        "stop_conditions": mission.get("stop_conditions") if isinstance(mission.get("stop_conditions"), list) else [],
    }


def _source_action_cards(text: str) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for line in text.splitlines():
        heading = re.match(r"^###\s+(?:\d+\.\s+)?(.+?)\s*$", line)
        if heading:
            if current:
                cards.append(current)
            title = heading.group(1).strip()
            current = {"title": title, "fields": {}, "raw": []}
            continue
        if current is None:
            continue
        current["raw"].append(line)
        field = re.match(r"^\s*-\s+([^:]+):\s+(.*)$", line)
        if field:
            key = _norm_heading(_strip_md(field.group(1))).replace(" ", "_")
            current["fields"][key] = _strip_md(field.group(2))
    if current:
        cards.append(current)

    for card in cards:
        fields = card["fields"]
        card["product"] = fields.get("product", "")
        card["path"] = fields.get("path", "")
        card["source"] = fields.get("source", "")
        card["status"] = fields.get("status", "")
        card["issue"] = fields.get("issue", "")
        card["first_check"] = fields.get("first_check", "")
        card["likely_fix"] = fields.get("likely_fix", "")
        card["verification"] = fields.get("verification", "")
        card["stop_rule"] = fields.get("stop_rule", "")
        card["raw"] = "\n".join(card["raw"]).strip()
    return cards
