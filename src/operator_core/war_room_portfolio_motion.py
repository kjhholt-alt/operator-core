"""Read-only portfolio motion, side-project, and Forge state."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def collect_portfolio_motion(war_room_dir: Path) -> dict[str, Any]:
    project_motion_path = war_room_dir / "project-motion-board.json"
    side_projects_path = war_room_dir / "side-projects-portfolio-os.json"
    side_next_builds_path = war_room_dir / "SIDE_PROJECTS_NEXT_BUILDS.md"
    next_build_path = war_room_dir / "NEXT_BUILD_CARD.md"
    forge_path = war_room_dir / "forge.json"
    forge_runs_path = war_room_dir / "forge-runs.jsonl"

    project_motion = _read_json(project_motion_path, {})
    side_projects = _read_json(side_projects_path, {})
    forge = _read_json(forge_path, {})
    forge_runs = _read_jsonl(forge_runs_path)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "project_motion": _project_motion_summary(project_motion if isinstance(project_motion, dict) else {}),
        "side_projects": _side_project_summary(side_projects if isinstance(side_projects, dict) else {}),
        "side_next_builds": _side_next_builds(_read_text(side_next_builds_path)),
        "next_build_card": _next_build_card(_read_text(next_build_path)),
        "forge": _forge_summary(forge if isinstance(forge, dict) else {}, forge_runs),
        "artifacts": {
            "project_motion": _meta(project_motion_path),
            "side_projects": _meta(side_projects_path),
            "side_next_builds": _meta(side_next_builds_path),
            "next_build_card": _meta(next_build_path),
            "forge": _meta(forge_path),
            "forge_runs": _meta(forge_runs_path),
        },
    }


def _project_motion_summary(doc: dict[str, Any]) -> dict[str, Any]:
    projects = doc.get("projects") if isinstance(doc.get("projects"), list) else []
    lanes = doc.get("lanes") if isinstance(doc.get("lanes"), list) else []
    top_mover = doc.get("top_mover") if isinstance(doc.get("top_mover"), dict) else {}
    stale_watch = doc.get("stale_watch") if isinstance(doc.get("stale_watch"), list) else []
    stream = doc.get("stream") if isinstance(doc.get("stream"), list) else []
    return {
        "mode": str(doc.get("mode") or ""),
        "purpose": str(doc.get("purpose") or ""),
        "lanes": [_pick(item, "lane", "count", "projects") for item in lanes if isinstance(item, dict)],
        "top_mover": _project_card(top_mover),
        "projects": [_project_card(item) for item in sorted(projects, key=lambda p: int(p.get("motion_score") or 0), reverse=True)[:12] if isinstance(item, dict)],
        "stale_watch": stale_watch[:12],
        "stream": [_pick(item, "project", "lane", "kind", "title", "evidence", "when", "weight") for item in stream[:20] if isinstance(item, dict)],
    }


def _project_card(item: dict[str, Any]) -> dict[str, Any]:
    latest_run = item.get("latest_run") if isinstance(item.get("latest_run"), dict) else {}
    return {
        "id": str(item.get("id") or ""),
        "title": str(item.get("title") or ""),
        "path": str(item.get("path") or ""),
        "lane": str(item.get("lane") or ""),
        "temperature": str(item.get("temperature") or ""),
        "motion_score": _int(item.get("motion_score")),
        "mention_count": _int(item.get("mention_count")),
        "event_count": _int(item.get("event_count")),
        "last_moved": str(item.get("last_moved") or ""),
        "next_action": str(item.get("next_action") or ""),
        "evidence": str(item.get("evidence") or ""),
        "latest_run": _pick(latest_run, "mission", "grade", "duration_minutes", "score", "verdict"),
    }


def _side_project_summary(doc: dict[str, Any]) -> dict[str, Any]:
    projects = doc.get("projects") if isinstance(doc.get("projects"), list) else []
    top_slugs = doc.get("top_builds") if isinstance(doc.get("top_builds"), list) else []
    by_slug = {str(item.get("slug")): item for item in projects if isinstance(item, dict)}
    top_projects = [by_slug[slug] for slug in top_slugs if slug in by_slug]
    if not top_projects:
        top_projects = sorted(projects, key=lambda p: int(p.get("buildScore") or 0), reverse=True)[:8]
    return {
        "mode": str(doc.get("mode") or ""),
        "project_count": _int(doc.get("project_count") or len(projects)),
        "codex_count": _int(doc.get("codex_count")),
        "claude_count": _int(doc.get("claude_count")),
        "average_readiness": _int(doc.get("average_readiness")),
        "top_builds": [str(slug) for slug in top_slugs[:10]],
        "projects": [_side_project_card(item) for item in top_projects[:10] if isinstance(item, dict)],
        "safety": doc.get("safety") if isinstance(doc.get("safety"), list) else [],
    }


def _side_project_card(item: dict[str, Any]) -> dict[str, Any]:
    readiness = item.get("readiness") if isinstance(item.get("readiness"), dict) else {}
    git = item.get("git") if isinstance(item.get("git"), dict) else {}
    return {
        "slug": str(item.get("slug") or ""),
        "title": str(item.get("title") or ""),
        "owner": str(item.get("owner") or ""),
        "status": str(item.get("status") or ""),
        "tier": str(item.get("tier") or ""),
        "build_score": _int(item.get("buildScore")),
        "energy_mode": str(item.get("energyMode") or ""),
        "next_action": str(item.get("nextAction") or ""),
        "tagline": str(item.get("tagline") or ""),
        "readiness_score": _int(readiness.get("score")),
        "readiness_grade": str(readiness.get("grade") or ""),
        "blockers": readiness.get("blockers") if isinstance(readiness.get("blockers"), list) else [],
        "git_clean": bool(git.get("clean")) if "clean" in git else None,
    }


def _side_next_builds(text: str) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for line in text.splitlines():
        heading = re.match(r"^##\s+\d+\.\s+(.+?)\s*$", line)
        if heading:
            if current:
                cards.append(current)
            current = {"title": heading.group(1).strip()}
            continue
        if current is None:
            continue
        field = re.match(r"^\s*-\s+([^:]+):\s*(.*)$", line)
        if field:
            current[_norm(field.group(1))] = _strip_md(field.group(2))
    if current:
        cards.append(current)
    return cards[:8]


def _next_build_card(text: str) -> dict[str, Any]:
    return {
        "build": _first_bullet(_section_text(text, "Build")),
        "source": _bullet_section(text, "Source"),
        "spec": _first_paragraph(_section_text(text, "Spec")),
        "test_plan": _first_paragraph(_section_text(text, "Test Plan")),
        "acceptance_checks": _bullet_section(text, "Acceptance Checks"),
        "next_action": _first_bullet(_section_text(text, "Next Action")) or _first_paragraph(_section_text(text, "Next Action")),
        "stop_rule": _first_bullet(_section_text(text, "Stop Rule")) or _first_paragraph(_section_text(text, "Stop Rule")),
        "safety": _bullet_section(text, "Safety"),
    }


def _forge_summary(doc: dict[str, Any], jsonl_runs: list[dict[str, Any]]) -> dict[str, Any]:
    summary = doc.get("summary") if isinstance(doc.get("summary"), dict) else {}
    proposals = doc.get("proposals") if isinstance(doc.get("proposals"), list) else []
    runs = jsonl_runs or (doc.get("runs") if isinstance(doc.get("runs"), list) else [])
    promotion = doc.get("promotion_recommendation") if isinstance(doc.get("promotion_recommendation"), dict) else {}
    gates = doc.get("gate_library") if isinstance(doc.get("gate_library"), list) else []
    return {
        "version": str(doc.get("version") or ""),
        "generated_at": str(doc.get("generated_at") or ""),
        "summary": summary,
        "promotion_recommendation": _pick(promotion, "proposal_id", "title", "why", "first_action", "verification"),
        "ready_proposals": [
            _proposal_card(item)
            for item in proposals
            if isinstance(item, dict) and str(item.get("status") or "").lower() == "ready"
        ][:8],
        "proposals": [_proposal_card(item) for item in proposals[:10] if isinstance(item, dict)],
        "runs": [_pick(item, "id", "proposal_id", "project", "status", "result", "updated_at", "verification") for item in runs[:10] if isinstance(item, dict)],
        "gates": [_pick(item, "id", "name", "rule") for item in gates[:8] if isinstance(item, dict)],
    }


def _proposal_card(item: dict[str, Any]) -> dict[str, Any]:
    scores = item.get("scores") if isinstance(item.get("scores"), dict) else {}
    return {
        "id": str(item.get("id") or ""),
        "pillar": str(item.get("pillar") or ""),
        "title": str(item.get("title") or ""),
        "status": str(item.get("status") or ""),
        "problem": str(item.get("problem") or ""),
        "first_build": str((item.get("build") or [""])[0]) if isinstance(item.get("build"), list) else "",
        "verification": item.get("verification") if isinstance(item.get("verification"), list) else [],
        "forge_score": _int(scores.get("forge_score")),
    }


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


def _bullet_section(text: str, heading: str) -> list[str]:
    section = _section_text(text, heading)
    return [_strip_md(line.strip()[2:]) for line in section.splitlines() if line.strip().startswith("- ")]


def _first_bullet(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            return _strip_md(stripped[2:])
    return ""


def _first_paragraph(text: str) -> str:
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            if lines:
                break
            continue
        if stripped.startswith("- "):
            continue
        lines.append(_strip_md(stripped))
    return " ".join(lines)


def _strip_md(text: str) -> str:
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    return text.replace("`", "").strip()


def _pick(doc: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: doc.get(key) for key in keys if key in doc}


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


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
