"""Regression tests for sprint + handoff tooling (Sprint 6, Phase C).

Covers:
  - Sprint state round-trip (start → load → clear).
  - `operator sprint start` records git heads on a real test repo.
  - Elapsed-minutes math + sweet-spot banner thresholds.
  - Handoff template includes the sprint goal, shipped commits, and the
    rehydrate paste-blob.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


def _init_repo(path: Path) -> None:
    """Tiny git repo with a single commit, no signing, no GPG."""
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"], cwd=path, check=True
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"], cwd=path, check=True
    )
    subprocess.run(
        ["git", "config", "commit.gpgsign", "false"], cwd=path, check=True
    )
    (path / "README.md").write_text("r0", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=path, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "init"], cwd=path, check=True
    )


def _git_available() -> bool:
    try:
        subprocess.run(
            ["git", "--version"], capture_output=True, check=True, timeout=2
        )
        return True
    except (FileNotFoundError, subprocess.CalledProcessError, OSError,
            subprocess.TimeoutExpired):
        return False


def _make_settings(tmp_path: Path, project_paths: list[Path]):
    from operator_core.settings import (
        DaemonConfig, DeployConfig, HealthConfig, ProjectConfig, Settings,
    )

    return Settings(
        config_path=tmp_path / "c.toml",
        data_dir=tmp_path / "data",
        projects_root=tmp_path,
        worktrees_dir=tmp_path / "wt",
        github_handle="x",
        daemon=DaemonConfig(bind="127.0.0.1", port=8765),
        discord_channels={},
        projects=[
            ProjectConfig(
                slug=p.name,
                path=p,
                repo=f"x/{p.name}",
                type="python",
                deploy=DeployConfig(provider="vercel", url=f"https://{p.name}.x"),
                health=HealthConfig(path="/", expected_status=200),
                checks=[],
                autonomy_tier="low",
                protected_patterns=[],
                auto_merge=False,
            )
            for p in project_paths
        ],
    )


def test_sprint_state_roundtrip(tmp_path):
    """save_state / load_state / clear_state should round-trip cleanly."""
    from operator_core import sprint as sprint_mod

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    assert sprint_mod.load_state(data_dir) is None

    state = sprint_mod.SprintState(
        goal="ship Phase C",
        started_at_iso="2026-04-17T12:00:00+00:00",
        git_heads={"operator-core": "abc123def4"},
        branches={"operator-core": "main"},
        title="S6-C",
    )
    path = sprint_mod.save_state(state, data_dir)
    assert path.exists()

    loaded = sprint_mod.load_state(data_dir)
    assert loaded is not None
    assert loaded.goal == "ship Phase C"
    assert loaded.git_heads == {"operator-core": "abc123def4"}
    assert loaded.branches == {"operator-core": "main"}
    assert loaded.title == "S6-C"

    sprint_mod.clear_state(data_dir)
    assert sprint_mod.load_state(data_dir) is None


@pytest.mark.skipif(not _git_available(), reason="git not installed")
def test_sprint_start_records_real_git_head(tmp_path):
    """start_sprint should record the real HEAD SHA of each tracked project."""
    from operator_core import sprint as sprint_mod

    repo = tmp_path / "demo-repo"
    repo.mkdir()
    _init_repo(repo)

    settings = _make_settings(tmp_path, [repo])
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    state, created = sprint_mod.start_sprint(
        "exercise tests", settings=settings, data_dir=data_dir
    )
    assert created is True
    assert state.goal == "exercise tests"
    assert "demo-repo" in state.git_heads
    sha = state.git_heads["demo-repo"]
    assert len(sha) == 40  # full SHA-1

    # Idempotent: a second start returns the existing state.
    again, created2 = sprint_mod.start_sprint(
        "something else", settings=settings, data_dir=data_dir
    )
    assert created2 is False
    assert again.goal == "exercise tests"  # unchanged
    assert again.started_at_iso == state.started_at_iso


def test_elapsed_minutes_and_sweet_spot_banner():
    from operator_core import sprint as sprint_mod

    started = datetime(2026, 4, 17, 12, 0, 0, tzinfo=timezone.utc)
    state = sprint_mod.SprintState(
        goal="x", started_at_iso=started.isoformat()
    )

    assert sprint_mod.elapsed_minutes(
        state, now=started + timedelta(minutes=30)
    ) == pytest.approx(30.0, abs=0.01)

    # Under the warning threshold: no banner.
    assert sprint_mod.sweet_spot_banner(30) is None
    # Approaching: 70 → "approaching" banner.
    banner_mid = sprint_mod.sweet_spot_banner(75)
    assert banner_mid is not None and "approaching" in banner_mid.lower()
    # Past the sweet spot.
    banner_late = sprint_mod.sweet_spot_banner(120)
    assert banner_late is not None and "past" in banner_late.lower()


@pytest.mark.skipif(not _git_available(), reason="git not installed")
def test_generate_handoff_file_writes_md_with_required_sections(tmp_path):
    """The generated handoff must have a goal, the 'what shipped' section,
    the rehydrate block, and a paste-blob."""
    from operator_core import sprint as sprint_mod

    repo = tmp_path / "proj-a"
    repo.mkdir()
    _init_repo(repo)

    settings = _make_settings(tmp_path, [repo])
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    state, _ = sprint_mod.start_sprint(
        "phase c landing", settings=settings, data_dir=data_dir
    )

    # Land one more commit so there's something "shipped" since the sprint
    # started.
    (repo / "NEW.md").write_text("new", encoding="utf-8")
    subprocess.run(["git", "add", "NEW.md"], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "ship NEW.md"], cwd=repo, check=True
    )

    path, body = sprint_mod.generate_handoff_file(
        state=state,
        settings=settings,
        projects_root=tmp_path,
        title="Sprint 6 phase C test handoff",
    )
    assert path.exists()
    assert path.name.startswith("HANDOFF_") and path.suffix == ".md"

    # Required sections.
    assert "phase c landing" in body  # goal
    assert "## What shipped" in body
    assert "ship NEW.md" in body  # the post-start commit appears
    assert "## Rehydrate commands" in body
    assert "operator doctor" in body
    assert "## Paste-blob for the fresh session" in body
    assert "Do not re-plan" in body  # paste-blob boilerplate
    assert "proj-a" in body  # per-project heading

    # `newest_handoff` should now find this file.
    newest = sprint_mod.newest_handoff(tmp_path)
    assert newest == path
    resumed = sprint_mod.resume_text(tmp_path)
    assert resumed is not None and "phase c landing" in resumed


def test_render_handoff_handles_no_sprint_state(tmp_path):
    """render_handoff must still produce a valid doc when no sprint is
    active — useful for ad-hoc `operator handoff` calls."""
    from operator_core import sprint as sprint_mod

    settings = _make_settings(tmp_path, [])
    body = sprint_mod.render_handoff(
        state=None,
        settings=settings,
        projects_root=tmp_path,
        title="Ad-hoc handoff",
    )
    assert "# Ad-hoc handoff" in body
    assert "## Rehydrate commands" in body
    assert "## Paste-blob" in body
