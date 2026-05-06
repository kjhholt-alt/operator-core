"""Local launch-prep artifacts for action packets.

This module deliberately prepares launch records only. It does not start an
agent process, call external APIs, or perform repository mutations.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def agent_launch_dir(data_dir: Path | None = None) -> Path:
    override = os.environ.get("OPERATOR_AGENT_LAUNCH_DIR")
    if override:
        return Path(override)
    base = data_dir if data_dir is not None else Path.home() / ".operator" / "data"
    return base / "agent_launches"


def list_agent_launches(launch_dir: Path, *, limit: int = 50) -> list[dict[str, Any]]:
    if not launch_dir.exists():
        return []
    launches: list[dict[str, Any]] = []
    for path in launch_dir.glob("*.json"):
        try:
            launch = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(launch, dict):
            continue
        launch.setdefault("paths", {})
        if isinstance(launch["paths"], dict):
            launch["paths"].setdefault("json", str(path))
            launch["paths"].setdefault("markdown", str(path.with_suffix(".md")))
        launches.append(launch)
    launches.sort(key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)
    return launches[:limit]


def launch_summary(launches: list[dict[str, Any]]) -> dict[str, Any]:
    by_status: dict[str, int] = {}
    for launch in launches:
        status = str(launch.get("status") or "prepared")
        by_status[status] = by_status.get(status, 0) + 1
    return {
        "count": len(launches),
        "by_status": by_status,
        "prepared_count": by_status.get("prepared", 0),
    }


def find_launch_by_packet(launch_dir: Path, packet_id: str) -> dict[str, Any] | None:
    packet_id = str(packet_id or "")
    if not packet_id:
        return None
    for launch in list_agent_launches(launch_dir, limit=500):
        if launch.get("packet_id") == packet_id:
            return launch
    return None


def prepare_agent_launch(
    packet: dict[str, Any],
    launch_dir: Path,
    *,
    job_store: Any | None = None,
    actor: str = "cockpit",
) -> dict[str, Any]:
    packet_id = str(packet.get("id") or "")
    if not packet_id:
        raise ValueError("packet id is required")
    existing = find_launch_by_packet(launch_dir, packet_id)
    if existing is not None:
        return existing

    now = _now()
    project = _packet_project(packet)
    launch_id = _clean_id(f"{now.replace('-', '').replace(':', '').replace('+00:00', 'Z')}-launch-{packet_id}")
    launch = {
        "id": launch_id,
        "packet_id": packet_id,
        "packet_title": packet.get("title") or "",
        "packet_kind": packet.get("kind") or "",
        "project": project,
        "status": "prepared",
        "created_at": now,
        "updated_at": now,
        "actor": actor,
        "safety": {
            "local_artifact_only": True,
            "starts_agent": False,
            "external_apis": False,
            "external_sends": False,
            "repository_mutations": False,
        },
        "prompt": _launch_prompt(packet),
        "job_id": "",
        "paths": {},
    }
    if job_store is not None:
        job = job_store.create_job(
            "agent.launch_prepared",
            prompt=launch["prompt"],
            project=project,
            metadata={
                "packet_id": packet_id,
                "launch_id": launch_id,
                "packet_kind": packet.get("kind"),
                "local_only": True,
            },
        )
        launch["job_id"] = job.id
    return _write_launch(launch, launch_dir)


def _write_launch(launch: dict[str, Any], launch_dir: Path) -> dict[str, Any]:
    launch_dir.mkdir(parents=True, exist_ok=True)
    launch_id = _clean_id(str(launch["id"]))
    json_path = launch_dir / f"{launch_id}.json"
    markdown_path = launch_dir / f"{launch_id}.md"
    launch["paths"] = {"json": str(json_path), "markdown": str(markdown_path)}
    _atomic_write_text(markdown_path, _render_launch_markdown(launch))
    _atomic_write_text(json_path, json.dumps(launch, indent=2, sort_keys=True) + "\n")
    return launch


def _render_launch_markdown(launch: dict[str, Any]) -> str:
    return "\n".join([
        f"# Launch Prep: {launch.get('packet_title')}",
        "",
        "## Metadata",
        "",
        f"- Launch ID: `{launch.get('id')}`",
        f"- Packet ID: `{launch.get('packet_id')}`",
        f"- Project: `{launch.get('project')}`",
        f"- Status: `{launch.get('status')}`",
        f"- Job ID: `{launch.get('job_id')}`",
        "",
        "## Safety",
        "",
        "- Local artifact only.",
        "- Does not start an agent.",
        "- Does not call external APIs.",
        "- Does not send messages.",
        "- Does not mutate repositories.",
        "",
        "## Prompt",
        "",
        "```text",
        str(launch.get("prompt") or ""),
        "```",
        "",
    ])


def _launch_prompt(packet: dict[str, Any]) -> str:
    context = packet.get("context") if isinstance(packet.get("context"), dict) else {}
    source_event = context.get("source_event") if isinstance(context.get("source_event"), dict) else {}
    return "\n".join([
        f"Packet: {packet.get('title') or packet.get('id')}",
        f"Kind: {packet.get('kind')}",
        f"Project: {_packet_project(packet)}",
        "",
        "Goal:",
        str(context.get("summary") or packet.get("title") or ""),
        "",
        "Source event:",
        json.dumps(source_event, indent=2, sort_keys=True),
        "",
        "Safety:",
        "- Work locally.",
        "- Do not send external messages.",
        "- Do not delete files.",
        "- Stop before production or credential changes.",
    ])


def _packet_project(packet: dict[str, Any]) -> str:
    context = packet.get("context") if isinstance(packet.get("context"), dict) else {}
    source_event = context.get("source_event") if isinstance(context.get("source_event"), dict) else {}
    return _slug(str(context.get("project") or source_event.get("project") or "operator-core"))


def _clean_id(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip(".-")
    if not cleaned:
        raise ValueError("launch id is required")
    return cleaned[:200]


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", str(value).lower()).strip("-") or "operator-core"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write_text(path: Path, body: str) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(body, encoding="utf-8")
    tmp.replace(path)
