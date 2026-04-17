"""Tests for `operator demo briefing`.

The briefing must be demo-safe: run in <5 seconds on a cold terminal with
no projects configured, no Supabase creds, no network. Output should
contain the header, project-heartbeat section, deploys-ticker section, and
a [no data] badge (not a traceback) when a data source is unreachable.
"""

from __future__ import annotations

import io
from contextlib import redirect_stdout

import pytest


def test_briefing_runs_with_no_settings(monkeypatch):
    """When load_settings fails, the briefing still prints and exits 0."""
    from operator_core import demo as demo_mod

    # Force ANSI off so the captured string is easy to assert on.
    monkeypatch.setattr(demo_mod, "_COLOR", False)

    # Make the Supabase fetch return [] deterministically.
    monkeypatch.setattr(demo_mod, "_fetch_recent_deploys", lambda limit=3: [])

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = demo_mod.run_briefing(None)

    out = buf.getvalue()
    assert rc == 0
    assert "OPERATOR LIVE BRIEFING" in out
    assert "PROJECT HEARTBEAT" in out
    assert "DEPLOYS TICKER" in out
    assert "[no data]" in out  # graceful fallback appears somewhere
    assert "operator handoff" in out  # footer hint


def test_briefing_renders_project_rows(monkeypatch, tmp_path):
    """With a Settings object carrying one project path, the heartbeat
    section should list that slug (commits=0 since tmp isn't a real repo)."""
    from operator_core import demo as demo_mod
    from operator_core.settings import (
        DaemonConfig, DeployConfig, HealthConfig, ProjectConfig, Settings,
    )

    monkeypatch.setattr(demo_mod, "_COLOR", False)
    monkeypatch.setattr(demo_mod, "_fetch_recent_deploys", lambda limit=3: [])
    # Make git calls deterministic: no data.
    monkeypatch.setattr(demo_mod, "_commits_last_7d", lambda p: 0)
    monkeypatch.setattr(demo_mod, "_last_commit", lambda p: "no commits")
    monkeypatch.setattr(demo_mod, "_recent_commits_oneline", lambda p, limit=5: [])

    proj = tmp_path / "demo-slug"
    proj.mkdir()
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
                slug="demo-slug",
                path=proj,
                repo="x/demo-slug",
                type="python",
                deploy=DeployConfig(
                    provider="vercel", url="https://example.test"
                ),
                health=HealthConfig(path="/", expected_status=200),
                checks=[],
                autonomy_tier="low",
                protected_patterns=[],
                auto_merge=False,
            )
        ],
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = demo_mod.run_briefing(settings)
    out = buf.getvalue()

    assert rc == 0
    assert "demo-slug" in out
    assert "example.test" in out
    # Zero-commit projects get the empty pip; make sure no crash.
    assert "[ ]" in out or "0 commits" in out
