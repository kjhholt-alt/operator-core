"""Read-only skills, QA, evaluations, and run-history state."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def collect_quality_history(war_room_dir: Path) -> dict[str, Any]:
    skills_path = war_room_dir / "kruz-skills.json"
    skill_runs_path = war_room_dir / "kruz-skill-runs.jsonl"
    skill_proposals_path = war_room_dir / "kruz-skill-proposals.jsonl"
    all_pages_qa_path = war_room_dir / "all-pages-qa.json"
    gauntlet_path = war_room_dir / "gauntlet-report.json"
    agent_runs_path = war_room_dir / "agent-runs.jsonl"
    runlog_path = war_room_dir / "RUNLOG.md"
    evaluations_dir = war_room_dir / "evaluations"

    skills = _read_json(skills_path, {})
    skill_runs = _read_jsonl(skill_runs_path)
    skill_proposals = _read_jsonl(skill_proposals_path)
    all_pages_qa = _read_json(all_pages_qa_path, {})
    gauntlet = _read_json(gauntlet_path, {})
    agent_runs = _read_jsonl(agent_runs_path)
    evaluation_files = _evaluation_files(evaluations_dir)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "skills": _skills_summary(skills if isinstance(skills, dict) else {}, skill_runs, skill_proposals),
        "qa": _qa_summary(all_pages_qa if isinstance(all_pages_qa, dict) else {}, gauntlet if isinstance(gauntlet, dict) else {}, evaluation_files),
        "run_history": _run_history(agent_runs),
        "artifacts": {
            "skills": _meta(skills_path),
            "skill_runs": _meta(skill_runs_path),
            "skill_proposals": _meta(skill_proposals_path),
            "all_pages_qa": _meta(all_pages_qa_path),
            "gauntlet": _meta(gauntlet_path),
            "agent_runs": _meta(agent_runs_path),
            "runlog": _meta(runlog_path),
            "evaluations_dir": _meta(evaluations_dir),
        },
    }


def _skills_summary(doc: dict[str, Any], runs: list[dict[str, Any]], proposals: list[dict[str, Any]]) -> dict[str, Any]:
    skills = doc.get("skills") if isinstance(doc.get("skills"), list) else []
    return {
        "version": str(doc.get("version") or ""),
        "purpose": str(doc.get("purpose") or ""),
        "skill_count": len(skills),
        "principles": doc.get("principles") if isinstance(doc.get("principles"), list) else [],
        "skills": [_pick(item, "id", "name", "domain", "use_when", "triggers") for item in skills[:12] if isinstance(item, dict)],
        "run_count": len(runs),
        "latest_runs": [_skill_run(item) for item in runs[-8:]],
        "proposal_count": len(proposals),
        "latest_proposals": proposals[-8:],
    }


def _skill_run(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": str(item.get("timestamp") or item.get("ts") or ""),
        "agent": str(item.get("agent") or ""),
        "skill_id": str(item.get("skill_id") or ""),
        "task": str(item.get("task") or ""),
        "total": _int(item.get("total")),
        "max": _int(item.get("max")),
        "lesson": str(item.get("lesson") or ""),
    }


def _qa_summary(all_pages: dict[str, Any], gauntlet: dict[str, Any], evaluation_files: list[dict[str, Any]]) -> dict[str, Any]:
    pages = all_pages.get("pages") if isinstance(all_pages.get("pages"), list) else []
    failed_pages = [p for p in pages if isinstance(p, dict) and str(p.get("status") or "").lower() not in {"pass", "ok"}]
    return {
        "all_pages": {
            "status": str(all_pages.get("status") or ""),
            "page_count": _int(all_pages.get("page_count") or len(pages)),
            "pass_count": _int(all_pages.get("pass_count")),
            "warn_count": _int(all_pages.get("warn_count")),
            "missing_count": _int(all_pages.get("missing_count")),
            "browser_qa_status": str(all_pages.get("browser_qa_status") or ""),
            "failed_pages": [_pick(item, "id", "title", "status", "file") for item in failed_pages[:8]],
        },
        "gauntlet": _pick(gauntlet, "date", "mission", "verdict", "lesson", "next_experiment"),
        "gauntlet_scores": {
            "flow": (gauntlet.get("flow_score") or {}).get("total") if isinstance(gauntlet.get("flow_score"), dict) else None,
            "flow_max": (gauntlet.get("flow_score") or {}).get("max") if isinstance(gauntlet.get("flow_score"), dict) else None,
            "universal": (gauntlet.get("universal_score") or {}).get("total") if isinstance(gauntlet.get("universal_score"), dict) else None,
            "universal_max": (gauntlet.get("universal_score") or {}).get("max") if isinstance(gauntlet.get("universal_score"), dict) else None,
        },
        "evaluation_count": len(evaluation_files),
        "evaluation_files": evaluation_files[:12],
    }


def _run_history(runs: list[dict[str, Any]]) -> dict[str, Any]:
    latest = list(reversed(runs[-10:]))
    return {
        "run_count": len(runs),
        "latest_runs": [
            _pick(item, "id", "date", "agent", "project", "mission", "verdict", "next_sprint", "artifacts", "verification", "scores")
            for item in latest
            if isinstance(item, dict)
        ],
    }


def _evaluation_files(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    files = []
    for path in sorted(root.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True):
        meta = _meta(path)
        meta["title"] = _markdown_title(path)
        files.append(meta)
    return files


def _markdown_title(path: Path) -> str:
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("# "):
                return line[2:].strip()
    except OSError:
        return ""
    return path.stem


def _pick(doc: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: doc.get(key) for key in keys if key in doc}


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
