"""`!op hooks status` command helper.

Separate module so `discord_bot.py` can import it later without this sprint
having to touch the bot file. Reads ~/.claude/settings.json (or a caller-
provided path) and reports which Operator V3 hook entries are currently
registered.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .hooks_installer import _HOOK_EVENT_KEYS, _signature

OPERATOR_HOOK_MARKER = "/hooks/claude/"


def _iter_entries(settings: dict[str, Any]):
    hooks = settings.get("hooks") or {}
    if not isinstance(hooks, dict):
        return
    for event in _HOOK_EVENT_KEYS:
        entries = hooks.get(event) or []
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict):
                yield event, entry


def hooks_status(settings_path: Path | None = None) -> dict[str, Any]:
    """Return a summary dict describing installed Operator V3 hooks."""

    if settings_path is None:
        settings_path = Path.home() / ".claude" / "settings.json"
    settings_path = Path(settings_path)

    if not settings_path.exists():
        return {
            "settings_path": str(settings_path).replace("\\", "/"),
            "exists": False,
            "operator_hooks": [],
            "other_hooks": 0,
        }

    try:
        data = json.loads(settings_path.read_text(encoding="utf-8") or "{}")
    except json.JSONDecodeError as exc:
        return {
            "settings_path": str(settings_path).replace("\\", "/"),
            "exists": True,
            "error": f"invalid json: {exc}",
            "operator_hooks": [],
            "other_hooks": 0,
        }

    operator_hooks: list[dict[str, str]] = []
    other = 0
    for event, entry in _iter_entries(data):
        matcher, cmd = _signature(entry)
        if OPERATOR_HOOK_MARKER in cmd:
            operator_hooks.append({"event": event, "matcher": matcher, "command": cmd})
        else:
            other += 1

    return {
        "settings_path": str(settings_path).replace("\\", "/"),
        "exists": True,
        "operator_hooks": operator_hooks,
        "other_hooks": other,
    }


def format_status(result: dict[str, Any]) -> str:
    """Human-readable rendering for Discord."""

    lines = [f"**Hooks status** ({result['settings_path']})"]
    if not result.get("exists"):
        lines.append("settings.json not found - hooks not installed")
        return "\n".join(lines)
    if "error" in result:
        lines.append(f"error reading settings.json: {result['error']}")
        return "\n".join(lines)

    ops = result.get("operator_hooks", [])
    if not ops:
        lines.append("No Operator V3 hooks registered.")
    else:
        lines.append(f"{len(ops)} Operator V3 hook(s):")
        for h in ops:
            lines.append(f"- {h['event']} `{h['matcher'] or '*'}`")
    lines.append(f"Other hooks present: {result.get('other_hooks', 0)}")
    return "\n".join(lines)
