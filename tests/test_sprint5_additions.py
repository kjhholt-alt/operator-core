"""Regression tests for the Sprint 5 additions.

Covers:
  - _LazyPath has the Path-like methods discord_bot depends on.
  - `operator tasks` enable/disable round-trips through schedule.json.
  - Snapshot v2 payload carries schema_version, tasks, git_activity,
    and cost_series_7d.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest


def test_lazypath_is_path_drop_in(tmp_path, monkeypatch):
    """_LazyPath must support the methods we rely on in discord_bot."""
    monkeypatch.setenv("OPERATOR_CONFIG", str(tmp_path / "c.toml"))
    (tmp_path / "c.toml").write_text(
        '[user]\ngithub = "x"\nprojects_root = "{}"\n'.format(
            tmp_path.as_posix()
        ),
        encoding="utf-8",
    )
    from operator_core import settings as settings_mod
    settings_mod.clear_cache()

    from operator_core.paths import _LazyPath

    target = tmp_path / "hello.txt"
    target.write_text("hi", encoding="utf-8")
    lazy = _LazyPath(lambda: target)

    assert lazy.exists()
    assert lazy.is_file()
    assert not lazy.is_dir()
    assert lazy.read_text() == "hi"
    assert lazy.read_bytes() == b"hi"
    assert lazy.resolve() == target
    assert lazy.name == "hello.txt"
    assert lazy.suffix == ".txt"
    assert lazy.stem == "hello"

    lazy.write_text("updated")
    assert target.read_text() == "updated"


def test_tasks_enable_disable_roundtrip(tmp_path):
    """Toggling a task writes to schedule.json and is_task_disabled reflects it."""
    from operator_core import scheduler as sched_mod

    cfg = tmp_path / "schedule.json"
    assert not sched_mod.is_task_disabled("morning-briefing", cfg)

    changed = sched_mod.disable_task("morning-briefing", cfg)
    assert changed is True
    assert sched_mod.is_task_disabled("morning-briefing", cfg)

    # Idempotent: disabling again does nothing.
    assert sched_mod.disable_task("morning-briefing", cfg) is False

    changed = sched_mod.enable_task("morning-briefing", cfg)
    assert changed is True
    assert not sched_mod.is_task_disabled("morning-briefing", cfg)


def test_list_all_tasks_merges_defaults_and_disabled(tmp_path):
    from operator_core import scheduler as sched_mod

    cfg = tmp_path / "schedule.json"
    state = tmp_path / "state.json"

    sched_mod.disable_task("pr-review", cfg)

    rows = sched_mod.list_all_tasks(state_path=state, config_path=cfg)
    by_key = {r["key"]: r for r in rows}

    # All six built-ins present.
    assert {"morning-briefing", "pr-review", "deploy-check",
            "marketing-pulse", "ag-market-pulse", "cost-report"} <= set(by_key)

    assert by_key["pr-review"]["enabled"] is False
    assert by_key["morning-briefing"]["enabled"] is True
    # Each row has the shape the CLI + snapshot expect.
    for r in rows:
        assert set(r.keys()) >= {
            "key", "action", "time", "cadence", "enabled",
            "last_run", "description", "kind",
        }


def test_snapshot_v2_schema(tmp_path, monkeypatch):
    """build_snapshot must emit schema_version=2 with the new sections."""
    from operator_core import snapshot as snap_mod
    from operator_core.settings import (
        DaemonConfig, DeployConfig, HealthConfig, ProjectConfig, Settings,
    )

    # Minimal Settings with one project that has no .git dir (git_activity
    # should return the "never" entry, not crash).
    proj_dir = tmp_path / "proj"
    proj_dir.mkdir()
    settings = Settings(
        config_path=tmp_path / "c.toml",
        data_dir=tmp_path / "data",
        projects_root=tmp_path,
        worktrees_dir=tmp_path / "wt",
        github_handle="x",
        daemon=DaemonConfig(bind="127.0.0.1", port=8765),
        discord_channels={},
        projects=[
            ProjectConfig(
                slug="proj",
                path=proj_dir,
                repo="x/proj",
                type="python",
                deploy=DeployConfig(provider="vercel", url="https://x.com"),
                health=HealthConfig(path="/", expected_status=200),
                checks=[],
                autonomy_tier="low",
                protected_patterns=[],
                auto_merge=False,
            )
        ],
    )
    settings.ensure_dirs()

    payload = snap_mod.build_snapshot(
        status_path=tmp_path / "no-status.json",
        db_path=tmp_path / "no-db.sqlite3",
        watchdog_config_path=tmp_path / "no-watchdog.json",
        settings=settings,
    )

    assert payload["schema_version"] == 2
    assert "tasks" in payload
    assert "git_activity" in payload
    assert "cost_series_7d" in payload
    assert len(payload["cost_series_7d"]) == 7  # zero-filled 7 days
    assert payload["summary"]["tasks_total"] == len(payload["tasks"])
    assert payload["summary"]["cost_7d_usd"] == 0.0


def test_pid_alive_rejects_garbage():
    """Cross-platform pid-alive probe must not throw on bogus input."""
    from operator_core.status_tui import _pid_alive

    assert _pid_alive(None) is False
    assert _pid_alive("not an int") is False
    assert _pid_alive(0) is False
    assert _pid_alive(-1) is False
    # A huge unlikely pid should report dead without raising.
    assert _pid_alive(999_999_999) is False
