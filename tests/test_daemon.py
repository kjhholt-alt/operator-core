"""Smoke tests for the daemon module.

Goals for Sprint 3:
  - daemon.run() bails cleanly when config is missing
  - Daemon.start() + Daemon.stop() is idempotent
  - SnapshotPublisherThread respects the stop_event without waiting
    the full interval
  - CLI `operator run --help` exits 0

We don't actually boot the whole HTTP + scheduler here — those have their
own test coverage. These tests validate the orchestration wiring.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from operator_core import daemon as daemon_mod
from operator_core.daemon import Daemon, SnapshotPublisherThread, run
from operator_core.settings import (
    DaemonConfig,
    Settings,
)


def _make_settings(tmp_path: Path) -> Settings:
    """Build a minimal Settings pointing at a tmp directory."""
    data_dir = tmp_path / "data"
    worktrees_dir = tmp_path / "worktrees"
    data_dir.mkdir(parents=True, exist_ok=True)
    worktrees_dir.mkdir(parents=True, exist_ok=True)
    projects_root = tmp_path / "projects"
    projects_root.mkdir(parents=True, exist_ok=True)
    return Settings(
        config_path=tmp_path / "config.toml",
        data_dir=data_dir,
        projects_root=projects_root,
        worktrees_dir=worktrees_dir,
        github_handle="test-user",
        daemon=DaemonConfig(bind="127.0.0.1", port=8799),
        discord_channels={},
        projects=[],
    )


def test_run_missing_config_exits_1(tmp_path, monkeypatch, capsys):
    """If the config file doesn't exist, run() exits 1 with a helpful message."""
    monkeypatch.setenv("OPERATOR_CONFIG", str(tmp_path / "missing.toml"))
    # clear the settings cache so the env var is picked up
    from operator_core import settings as settings_mod
    settings_mod.clear_cache()

    rc = run(no_discord=True, no_scheduler=True, no_snapshot=True, once=True)
    assert rc == 1
    captured = capsys.readouterr()
    assert "not found" in captured.err.lower() or "operator init" in captured.err.lower()


def test_daemon_start_stop_idempotent(tmp_path, monkeypatch):
    """Daemon.stop() called twice in a row is safe."""
    monkeypatch.setenv("OPERATOR_CONFIG", str(tmp_path / "missing.toml"))
    settings = _make_settings(tmp_path)

    # Patch out HTTP server to avoid binding a real port.
    with patch("operator_core.daemon.serve_http") as mock_serve:
        mock_http = MagicMock()
        mock_http.serve_forever = MagicMock()
        mock_http.shutdown = MagicMock()
        mock_serve.return_value = mock_http
        with patch("operator_core.daemon.register_remote_route"), patch(
            "operator_core.daemon.register_metrics_route"
        ):
            d = Daemon(
                settings,
                no_discord=True,
                no_scheduler=True,
                no_snapshot=True,
            )
            d.start()
            d.stop()
            # Second stop should be a no-op, not raise.
            d.stop()


def test_snapshot_thread_respects_stop_event(tmp_path):
    """Stopping the snapshot thread early should not wait a full interval."""
    settings = _make_settings(tmp_path)
    stop = threading.Event()

    # Patch both build_snapshot and publish so the thread runs without
    # touching state files or Supabase.
    with patch("operator_core.snapshot.build_snapshot", return_value={"summary": {"projects": 0, "jobs_24h": 0, "cost_24h_usd": 0}}), patch(
        "operator_core.snapshot.publish"
    ):
        thread = SnapshotPublisherThread(
            settings=settings, interval_sec=3600, stop_event=stop
        )
        start = time.time()
        thread.start()
        # Give the first publish a moment to happen, then stop.
        time.sleep(0.2)
        stop.set()
        thread.join(timeout=5.0)
        elapsed = time.time() - start

    assert not thread.is_alive(), "snapshot thread did not exit on stop_event"
    assert elapsed < 4.0, f"snapshot thread took too long to shut down ({elapsed:.1f}s)"


def test_cli_run_help_exits_0():
    """`operator run --help` is the canonical is-it-wired-up check."""
    from operator_core.cli import build_parser

    parser = build_parser()
    with pytest.raises(SystemExit) as ei:
        parser.parse_args(["run", "--help"])
    assert ei.value.code == 0


def test_daemon_config_cli_override_via_dataclass_replace(tmp_path, monkeypatch):
    """Passing --host / --port should replace the frozen DaemonConfig."""
    import dataclasses

    settings = _make_settings(tmp_path)
    original_port = settings.daemon.port
    replaced = dataclasses.replace(
        settings.daemon,
        bind="0.0.0.0",
        port=9123,
    )
    settings.daemon = replaced
    assert settings.daemon.bind == "0.0.0.0"
    assert settings.daemon.port == 9123
    assert settings.daemon.port != original_port
