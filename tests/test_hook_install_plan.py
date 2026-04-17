"""Tests for operator_core.hooks_installer.

These never touch the real ~/.claude/settings.json. Everything runs against
an in-memory dict or a tmp_path fixture.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from operator_core import hooks_installer
from operator_core.hooks_command import format_status, hooks_status


EXAMPLE = {
    "hooks": {
        "PreToolUse": [
            {
                "matcher": "Bash",
                "hooks": [
                    {
                        "type": "command",
                        "command": "curl -sS http://127.0.0.1:8765/hooks/claude/pre-tool-use",
                    }
                ],
            }
        ],
        "PostToolUse": [
            {
                "matcher": "*",
                "hooks": [
                    {
                        "type": "command",
                        "command": "curl -sS http://127.0.0.1:8765/hooks/claude/post-tool-use",
                    }
                ],
            }
        ],
    },
    "_comment": "docs, should be stripped by apply_merge",
}


def test_plan_merge_into_empty_settings():
    plan = hooks_installer.plan_merge({}, EXAMPLE)
    events = {a[0] for a in plan["added"]}
    assert events == {"PreToolUse", "PostToolUse"}
    assert plan["conflicts"] == []
    assert plan["merged"]["hooks"]["PreToolUse"][0]["matcher"] == "Bash"


def test_plan_merge_preserves_unrelated_user_keys():
    existing = {
        "theme": "dark",
        "model": "opus",
        "hooks": {
            "PreToolUse": [
                {
                    "matcher": "Read",
                    "hooks": [
                        {"type": "command", "command": "echo user-owned"}
                    ],
                }
            ]
        },
    }
    plan = hooks_installer.plan_merge(existing, EXAMPLE)
    merged = plan["merged"]

    # user keys survived
    assert merged["theme"] == "dark"
    assert merged["model"] == "opus"
    assert "theme" in plan["preserved"] and "model" in plan["preserved"]

    # user's existing PreToolUse entry is still there
    pre = merged["hooks"]["PreToolUse"]
    matchers = [e["matcher"] for e in pre]
    assert "Read" in matchers
    assert "Bash" in matchers  # added from example


def test_plan_merge_does_not_duplicate_on_rerun():
    first = hooks_installer.plan_merge({}, EXAMPLE)
    second = hooks_installer.plan_merge(first["merged"], EXAMPLE)
    assert second["added"] == []
    assert len(second["conflicts"]) == 2  # the two example hooks already there
    # same structure as after the first merge
    assert second["merged"]["hooks"] == first["merged"]["hooks"]


def test_compute_backup_path_format(tmp_path: Path):
    frozen = dt.datetime(2026, 4, 11, 3, 15, 7)
    out = hooks_installer.compute_backup_path(tmp_path, now=frozen)
    assert out.parent == tmp_path
    assert out.name == "settings.backup-20260411-031507.json"


def test_apply_merge_dry_run_does_not_write(tmp_path: Path):
    settings = tmp_path / "claude" / "settings.json"
    example = tmp_path / "example.json"
    example.write_text(json.dumps(EXAMPLE), encoding="utf-8")
    # settings.json does not exist

    result = hooks_installer.apply_merge(
        settings_path=settings,
        example_path=example,
        backup_dir=tmp_path / "claude",
        dry_run=True,
    )
    assert result["dry_run"] is True
    assert not settings.exists()
    assert len(result["added"]) == 2
    assert result["backup_path"].endswith(".json")


def test_apply_merge_apply_writes_and_backs_up(tmp_path: Path):
    settings = tmp_path / "claude" / "settings.json"
    settings.parent.mkdir(parents=True)
    settings.write_text(json.dumps({"theme": "dark"}), encoding="utf-8")

    example = tmp_path / "example.json"
    example.write_text(json.dumps(EXAMPLE), encoding="utf-8")

    frozen = dt.datetime(2026, 4, 11, 3, 15, 7)
    result = hooks_installer.apply_merge(
        settings_path=settings,
        example_path=example,
        backup_dir=tmp_path / "claude",
        dry_run=False,
        now=frozen,
    )
    assert result["dry_run"] is False

    # backup exists and contains the pre-merge content
    backup = Path(result["backup_path"])
    assert backup.exists()
    assert json.loads(backup.read_text(encoding="utf-8")) == {"theme": "dark"}

    # post-merge settings has both the user key and the new hooks
    written = json.loads(settings.read_text(encoding="utf-8"))
    assert written["theme"] == "dark"
    assert "PreToolUse" in written["hooks"]
    assert "PostToolUse" in written["hooks"]


def test_apply_merge_strips_example_comment_keys(tmp_path: Path):
    settings = tmp_path / "claude" / "settings.json"
    example = tmp_path / "example.json"
    example.write_text(json.dumps(EXAMPLE), encoding="utf-8")

    result = hooks_installer.apply_merge(
        settings_path=settings,
        example_path=example,
        backup_dir=tmp_path / "claude",
        dry_run=True,
    )
    # the comment key from EXAMPLE must not leak into the merged output
    assert "_comment" not in result["merged"]


def test_hooks_status_missing_file(tmp_path: Path):
    result = hooks_status(tmp_path / "nope.json")
    assert result["exists"] is False
    assert result["operator_hooks"] == []
    text = format_status(result)
    assert "not found" in text


def test_hooks_status_detects_operator_hooks(tmp_path: Path):
    path = tmp_path / "settings.json"
    merged = hooks_installer.plan_merge({}, EXAMPLE)["merged"]
    path.write_text(json.dumps(merged), encoding="utf-8")

    result = hooks_status(path)
    assert result["exists"] is True
    events = {h["event"] for h in result["operator_hooks"]}
    assert events == {"PreToolUse", "PostToolUse"}
    assert result["other_hooks"] == 0
    text = format_status(result)
    assert "Operator V3 hook" in text


def test_hooks_status_counts_non_operator_hooks(tmp_path: Path):
    path = tmp_path / "settings.json"
    settings = {
        "hooks": {
            "PreToolUse": [
                {
                    "matcher": "Read",
                    "hooks": [{"type": "command", "command": "echo user"}],
                }
            ]
        }
    }
    path.write_text(json.dumps(settings), encoding="utf-8")

    result = hooks_status(path)
    assert result["operator_hooks"] == []
    assert result["other_hooks"] == 1
