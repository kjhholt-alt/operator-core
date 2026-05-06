"""Local-only action packet artifacts for Operator Cockpit.

Action packets are draft work orders for humans or local agents. They do not
send messages, call external APIs, delete files, or execute commands.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PACKET_STATUSES = ("draft", "ready", "claimed", "done")


@dataclass(frozen=True)
class ActionPacketKind:
    id: str
    label: str
    goal: str
    allowed_actions: tuple[str, ...]
    stop_rules: tuple[str, ...]
    verification: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "goal": self.goal,
            "allowed_actions": list(self.allowed_actions),
            "stop_rules": list(self.stop_rules),
            "verification": list(self.verification),
        }


ACTION_PACKET_KINDS: dict[str, ActionPacketKind] = {
    "claude_audit_packet": ActionPacketKind(
        id="claude_audit_packet",
        label="Create Claude audit packet",
        goal="Audit recently shipped autonomous work and identify concrete fixes or follow-ups.",
        allowed_actions=(
            "Read linked local files and test output.",
            "Inspect diffs and summarize behavioral risks.",
            "Write findings and recommended next checks into this packet.",
        ),
        stop_rules=(
            "Stop before pushing code or opening PRs.",
            "Stop before contacting external services.",
            "Stop if credentials, customer data, or destructive commands are required.",
        ),
        verification=(
            "Each finding references a local path, PR, artifact, or test.",
            "Each recommended fix has a clear owner and verification step.",
        ),
    ),
    "codex_implementation_packet": ActionPacketKind(
        id="codex_implementation_packet",
        label="Create Codex implementation packet",
        goal="Turn a vetted next action into a scoped local implementation task.",
        allowed_actions=(
            "Edit files inside the stated write scope.",
            "Add or update focused tests for the changed behavior.",
            "Commit and push only when the local test gate passes.",
        ),
        stop_rules=(
            "Stop before broad unrelated refactors.",
            "Stop before deleting or reverting user-authored work.",
            "Stop before external sends or live production changes.",
        ),
        verification=(
            "Run the narrow test set for the touched modules.",
            "Run the relevant cockpit or recipe smoke check when UI/state changes.",
        ),
    ),
    "next_agent_handoff": ActionPacketKind(
        id="next_agent_handoff",
        label="Create next-agent handoff",
        goal="Package the current mission state so the next agent can continue without rediscovery.",
        allowed_actions=(
            "Summarize shipped work, open risks, and exact next commands.",
            "List read-first files and write scopes.",
            "Record stop rules and verification gates.",
        ),
        stop_rules=(
            "Stop before assigning overlapping write scopes.",
            "Stop if the handoff depends on unstated local context.",
        ),
        verification=(
            "A fresh agent can start from the packet without reading chat history.",
            "The packet names the current repo, branch, and test gate.",
        ),
    ),
    "source_action_work_order": ActionPacketKind(
        id="source_action_work_order",
        label="Create source-action work order",
        goal="Convert a source registry gap or source action card into a bounded local work order.",
        allowed_actions=(
            "Inspect source artifacts and connected cockpit targets.",
            "Define the missing connection or stale data fix.",
            "Write a local implementation plan with verification.",
        ),
        stop_rules=(
            "Stop before live outreach, emails, posts, or external sends.",
            "Stop before changing production credentials or remote data.",
        ),
        verification=(
            "Cockpit JSON shows the source as connected or the gap is explicitly tracked.",
            "Relevant route or collector tests cover the new connection.",
        ),
    ),
    "autonomy_checkpoint_draft": ActionPacketKind(
        id="autonomy_checkpoint_draft",
        label="Create autonomy checkpoint draft",
        goal="Draft a local checkpoint for a long-running autonomous work session.",
        allowed_actions=(
            "Summarize phase, evidence, next action, blockers, and stop rules.",
            "Point to local artifacts created during the run.",
        ),
        stop_rules=(
            "Stop before marking work done without evidence.",
            "Stop before launching new autonomous work from the draft.",
        ),
        verification=(
            "Checkpoint includes evidence paths or command results.",
            "Next action is concrete and timeboxed.",
        ),
    ),
    "weekly_review_follow_up": ActionPacketKind(
        id="weekly_review_follow_up",
        label="Create weekly review follow-up packet",
        goal="Turn no-human-review merges into a prioritized spot-check queue.",
        allowed_actions=(
            "Rank auto-merged PRs by size and risk.",
            "Write audit notes and targeted test recommendations.",
            "Create follow-up implementation tasks only as local drafts.",
        ),
        stop_rules=(
            "Stop before commenting on PRs or notifying anyone externally.",
            "Stop before modifying repos outside the stated review scope.",
        ),
        verification=(
            "Largest autonomous merges are listed first.",
            "Each follow-up has an owner, scope, and pass/fail check.",
        ),
    ),
}


def action_packet_kinds() -> list[dict[str, Any]]:
    return [kind.to_dict() for kind in ACTION_PACKET_KINDS.values()]


def action_packet_dir(data_dir: Path | None = None) -> Path:
    override = os.environ.get("OPERATOR_ACTION_PACKET_DIR")
    if override:
        return Path(override)
    base = data_dir if data_dir is not None else Path.home() / ".operator" / "data"
    return base / "action_packets"


def list_action_packets(packet_dir: Path, *, limit: int = 50) -> list[dict[str, Any]]:
    if not packet_dir.exists():
        return []
    packets: list[dict[str, Any]] = []
    for path in packet_dir.glob("*.json"):
        try:
            packet = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(packet, dict):
            continue
        packet.setdefault("paths", {})
        if isinstance(packet["paths"], dict):
            packet["paths"].setdefault("json", str(path))
            packet["paths"].setdefault("markdown", str(path.with_suffix(".md")))
        packets.append(packet)
    packets.sort(key=lambda p: str(p.get("updated_at") or p.get("created_at") or ""), reverse=True)
    return packets[:limit]


def action_packet_summary(packets: list[dict[str, Any]]) -> dict[str, Any]:
    by_status = {status: 0 for status in PACKET_STATUSES}
    for packet in packets:
        status = str(packet.get("status") or "draft")
        by_status[status if status in by_status else "draft"] += 1
    return {
        "count": len(packets),
        "by_status": by_status,
        "open_count": by_status["draft"] + by_status["ready"] + by_status["claimed"],
    }


def create_action_packet(
    *,
    kind: str,
    title: str | None = None,
    context: dict[str, Any] | None = None,
    packet_dir: Path,
    status: str = "draft",
) -> dict[str, Any]:
    template = _require_kind(kind)
    status = _require_status(status)
    now = _now()
    packet_id = _new_packet_id(template.id, title or template.label, now)
    packet = {
        "id": packet_id,
        "kind": template.id,
        "kind_label": template.label,
        "title": title or template.label,
        "status": status,
        "created_at": now,
        "updated_at": now,
        "context": context or {},
        "safety": {
            "local_files_only": True,
            "external_apis": False,
            "external_sends": False,
            "deletes": False,
        },
        "allowed_actions": list(template.allowed_actions),
        "stop_rules": list(template.stop_rules),
        "verification": list(template.verification),
        "paths": {},
    }
    return _write_packet(packet, packet_dir)


def update_action_packet_status(packet_id: str, status: str, packet_dir: Path) -> dict[str, Any]:
    packet_id = _clean_packet_id(packet_id)
    status = _require_status(status)
    path = packet_dir / f"{packet_id}.json"
    if not path.exists():
        raise FileNotFoundError(packet_id)
    packet = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(packet, dict):
        raise ValueError("packet metadata is not an object")
    packet["status"] = status
    packet["updated_at"] = _now()
    return _write_packet(packet, packet_dir)


def _write_packet(packet: dict[str, Any], packet_dir: Path) -> dict[str, Any]:
    packet_dir.mkdir(parents=True, exist_ok=True)
    packet_id = _clean_packet_id(str(packet["id"]))
    json_path = packet_dir / f"{packet_id}.json"
    markdown_path = packet_dir / f"{packet_id}.md"
    packet["paths"] = {"json": str(json_path), "markdown": str(markdown_path)}

    _atomic_write_text(markdown_path, _render_markdown(packet))
    _atomic_write_text(json_path, json.dumps(packet, indent=2, sort_keys=True) + "\n")
    return packet


def _render_markdown(packet: dict[str, Any]) -> str:
    context = packet.get("context") if isinstance(packet.get("context"), dict) else {}
    context_summary = context.get("summary") or context.get("context_summary") or ""
    context_json = json.dumps(context, indent=2, sort_keys=True)
    sections = [
        f"# {packet.get('title')}",
        "",
        "## Packet Metadata",
        "",
        f"- ID: `{packet.get('id')}`",
        f"- Kind: `{packet.get('kind')}`",
        f"- Status: `{packet.get('status')}`",
        f"- Created: `{packet.get('created_at')}`",
        f"- Updated: `{packet.get('updated_at')}`",
        "",
        "## Goal",
        "",
        str(ACTION_PACKET_KINDS[str(packet.get("kind"))].goal),
        "",
        "## Source Context",
        "",
        str(context_summary or "No context summary provided."),
        "",
        "```json",
        context_json,
        "```",
        "",
        "## Allowed Actions",
        "",
        *_bullet_lines(packet.get("allowed_actions")),
        "",
        "## Stop Rules",
        "",
        *_bullet_lines(packet.get("stop_rules")),
        "",
        "## Verification",
        "",
        *_bullet_lines(packet.get("verification")),
        "",
        "## Handoff",
        "",
        "- Owner:",
        "- Claimed at:",
        "- Result:",
        "- Follow-up:",
        "",
    ]
    return "\n".join(sections)


def _bullet_lines(items: Any) -> list[str]:
    if not isinstance(items, list):
        return ["- None recorded."]
    rows = [f"- {item}" for item in items if item]
    return rows or ["- None recorded."]


def _atomic_write_text(path: Path, body: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(body, encoding="utf-8")
    tmp.replace(path)


def _require_kind(kind: str) -> ActionPacketKind:
    try:
        return ACTION_PACKET_KINDS[str(kind)]
    except KeyError as exc:
        raise ValueError(f"unknown action packet kind: {kind}") from exc


def _require_status(status: str) -> str:
    status = str(status)
    if status not in PACKET_STATUSES:
        raise ValueError(f"unknown action packet status: {status}")
    return status


def _new_packet_id(kind: str, title: str, now: str) -> str:
    timestamp = now.replace("-", "").replace(":", "").replace("+00:00", "Z")
    timestamp = timestamp.replace(".", "")
    return _clean_packet_id(f"{timestamp}-{kind}-{_slug(title)}")


def _clean_packet_id(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip(".-")
    if not cleaned:
        raise ValueError("packet id is required")
    return cleaned[:180]


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-")
    return slug[:64] or "packet"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
