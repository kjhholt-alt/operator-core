"""Claude Code settings.json hook installer.

Merges the operator-v3 example hook config into the user's
~/.claude/settings.json without clobbering existing keys.

Two public entry points:

- ``plan_merge(existing, additions)`` — pure function, returns a merged dict
  plus a diff description. Used in tests.
- ``apply_merge(settings_path, example_path, backup_dir, dry_run)`` — reads
  files, writes a backup, writes the merged result. Returns a dict with the
  plan + paths touched.

All paths use forward slashes (Python ``pathlib`` handles the conversion on
Windows). No network. No side effects when ``dry_run=True``.
"""

from __future__ import annotations

import copy
import datetime as _dt
import json
from pathlib import Path
from typing import Any


# Keys under ``hooks`` that the example file is allowed to own. Any other
# top-level keys belong to the user and must be preserved verbatim.
_HOOK_EVENT_KEYS = ("PreToolUse", "PostToolUse", "Stop", "SubagentStop", "Notification")


def _signature(hook_entry: dict[str, Any]) -> tuple[str, str]:
    """Return a (matcher, command) signature used to dedupe hook entries."""

    matcher = str(hook_entry.get("matcher", ""))
    inner = hook_entry.get("hooks") or []
    cmd = ""
    if inner and isinstance(inner, list):
        first = inner[0] or {}
        cmd = str(first.get("command", ""))
    return matcher, cmd


def plan_merge(existing: dict[str, Any], additions: dict[str, Any]) -> dict[str, Any]:
    """Merge ``additions`` into ``existing`` without dropping user keys.

    Returns a dict with keys:

    - ``merged``    — the resulting settings dict
    - ``added``     — list of (event, matcher, command) tuples that were
      newly inserted
    - ``preserved`` — list of top-level keys from ``existing`` that survived
    - ``conflicts`` — list of (event, matcher, command) tuples that already
      existed and were NOT overwritten
    """

    merged: dict[str, Any] = copy.deepcopy(existing) if existing else {}
    preserved = [k for k in merged.keys() if k != "hooks"]

    existing_hooks = merged.get("hooks")
    if not isinstance(existing_hooks, dict):
        existing_hooks = {}
    merged["hooks"] = existing_hooks

    add_hooks = (additions or {}).get("hooks") or {}
    added: list[tuple[str, str, str]] = []
    conflicts: list[tuple[str, str, str]] = []

    for event, entries in add_hooks.items():
        if event not in _HOOK_EVENT_KEYS:
            # Ignore unknown hook events from the example.
            continue
        if not isinstance(entries, list):
            continue
        bucket = existing_hooks.get(event)
        if not isinstance(bucket, list):
            bucket = []
            existing_hooks[event] = bucket

        existing_sigs = {_signature(e) for e in bucket if isinstance(e, dict)}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            sig = _signature(entry)
            if sig in existing_sigs:
                conflicts.append((event, sig[0], sig[1]))
                continue
            bucket.append(copy.deepcopy(entry))
            existing_sigs.add(sig)
            added.append((event, sig[0], sig[1]))

    return {
        "merged": merged,
        "added": added,
        "preserved": preserved,
        "conflicts": conflicts,
    }


def compute_backup_path(backup_dir: Path, now: _dt.datetime | None = None) -> Path:
    """Return the timestamped backup filename inside ``backup_dir``."""

    ts = (now or _dt.datetime.now()).strftime("%Y%m%d-%H%M%S")
    return Path(backup_dir) / f"settings.backup-{ts}.json"


def _strip_comments(data: dict[str, Any]) -> dict[str, Any]:
    """Drop keys starting with ``_`` (example file documentation keys)."""

    return {k: v for k, v in data.items() if not str(k).startswith("_")}


def apply_merge(
    settings_path: Path,
    example_path: Path,
    backup_dir: Path,
    dry_run: bool = True,
    now: _dt.datetime | None = None,
) -> dict[str, Any]:
    """Read ``settings_path`` + ``example_path``, merge, optionally write.

    When ``dry_run`` is True no files are written. The returned dict always
    contains the plan so callers can diff / log.
    """

    settings_path = Path(settings_path)
    example_path = Path(example_path)
    backup_dir = Path(backup_dir)

    existing: dict[str, Any] = {}
    if settings_path.exists():
        existing = json.loads(settings_path.read_text(encoding="utf-8") or "{}")

    example_raw = json.loads(example_path.read_text(encoding="utf-8") or "{}")
    additions = _strip_comments(example_raw)

    plan = plan_merge(existing, additions)
    backup_path = compute_backup_path(backup_dir, now=now)

    result = {
        "settings_path": str(settings_path).replace("\\", "/"),
        "example_path": str(example_path).replace("\\", "/"),
        "backup_path": str(backup_path).replace("\\", "/"),
        "dry_run": bool(dry_run),
        "added": plan["added"],
        "preserved": plan["preserved"],
        "conflicts": plan["conflicts"],
        "merged": plan["merged"],
    }

    if dry_run:
        return result

    backup_dir.mkdir(parents=True, exist_ok=True)
    if settings_path.exists():
        backup_path.write_text(
            settings_path.read_text(encoding="utf-8"), encoding="utf-8"
        )
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text(
        json.dumps(plan["merged"], indent=2) + "\n", encoding="utf-8"
    )
    return result


def _cli(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Merge Operator V3 hooks into ~/.claude/settings.json")
    parser.add_argument("--settings", required=True, help="Path to settings.json")
    parser.add_argument("--example", required=True, help="Path to operator-v3-hooks.example.json")
    parser.add_argument("--backup-dir", required=True, help="Directory for backup file")
    parser.add_argument("--apply", action="store_true", help="Actually write the merged file")
    args = parser.parse_args(argv)

    result = apply_merge(
        Path(args.settings),
        Path(args.example),
        Path(args.backup_dir),
        dry_run=not args.apply,
    )
    # Drop merged dict from stdout summary to keep output small.
    summary = {k: v for k, v in result.items() if k != "merged"}
    summary["added_count"] = len(result["added"])
    summary["conflict_count"] = len(result["conflicts"])
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_cli())
