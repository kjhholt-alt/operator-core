"""Tests for the deploy_health canary runner + circuit-breaker.

No live HTTP. All `http_get` calls are injected. One test exists for the
live path but is skipped unless `OPERATOR_SMOKE_LIVE=1` is set, per spec.
"""

from __future__ import annotations

import importlib
import json
import os
import urllib.error
from pathlib import Path

import pytest

from operator_core.config import DeployConfig, HealthConfig, ProjectConfig


@pytest.fixture()
def canary_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Point Operator data dir at a scratch location and reload modules.

    operator-core wires data_dir through settings.toml; write a config
    pointing `[data].dir` at tmp, set `OPERATOR_CONFIG`, clear the settings
    cache, then reload paths + deploy_health so module-level constants pick
    up the tmp paths.
    """
    data_dir = tmp_path / ".operator"
    data_dir.mkdir()
    projects_root = tmp_path / "projects"
    projects_root.mkdir()

    config = tmp_path / "config.toml"
    config.write_text(
        f"""
[user]
github = "tester"
projects_root = "{projects_root.as_posix()}"

[data]
dir = "{data_dir.as_posix()}"
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("OPERATOR_CONFIG", str(config))

    from operator_core import settings as settings_mod
    settings_mod.clear_cache()

    from operator_core import paths as paths_module
    importlib.reload(paths_module)
    from operator_core import deploy_health as dh_module
    importlib.reload(dh_module)
    return dh_module, data_dir


def _project(slug: str = "demo", status: int = 200) -> ProjectConfig:
    return ProjectConfig(
        slug=slug,
        path=Path("/tmp") / slug,
        repo=f"owner/{slug}",
        type="saas",
        deploy=DeployConfig(provider="vercel", url="https://example.com"),
        health=HealthConfig(path="/health", expected_status=status),
        checks=[],
        autonomy_tier="guarded",
        protected_patterns=[],
    )


def test_canary_happy_path_logs_hop_and_does_not_trip(canary_env) -> None:
    dh, data_dir = canary_env
    calls: list[str] = []

    def fake_get(url: str, timeout: float) -> tuple[int, bytes]:
        calls.append(url)
        return 200, b"ok"

    result = dh.run_canary(_project(), http_get=fake_get)

    assert result.ok is True
    assert result.final_status == 200
    assert result.signature() == "ok"
    assert calls == ["https://example.com/health"]
    assert not dh.is_tripped()

    log_path = data_dir / "logs" / "deploy-health.jsonl"
    assert log_path.exists()
    record = json.loads(log_path.read_text(encoding="utf-8").splitlines()[-1])
    assert record["type"] == "canary"
    assert record["project"] == "demo"
    assert record["ok"] is True
    assert record["breaker_tripped"] is False
    assert record["hops"][0]["status"] == 200


def test_canary_failing_runs_trip_breaker_after_three(canary_env) -> None:
    dh, _ = canary_env

    def fake_fail(url: str, timeout: float) -> tuple[int, bytes]:
        return 502, b"bad gateway"

    for _ in range(2):
        result = dh.run_canary(_project(), http_get=fake_fail)
        assert result.ok is False
        assert result.signature() == "status:502"
        assert not dh.is_tripped()

    result = dh.run_canary(_project(), http_get=fake_fail)
    assert result.ok is False
    assert dh.is_tripped()

    # is_tripped helper stays true until reset
    assert dh.is_tripped()
    assert dh.reset_trip() is True
    assert not dh.is_tripped()
    assert dh.reset_trip() is False


def test_canary_different_failures_do_not_accumulate(canary_env) -> None:
    dh, _ = canary_env

    responses = iter([(502, b""), (503, b""), (504, b"")])

    def fake_alt(url: str, timeout: float) -> tuple[int, bytes]:
        return next(responses)

    for _ in range(3):
        dh.run_canary(_project(), http_get=fake_alt)

    # Three different signatures → no trip.
    assert not dh.is_tripped()


def test_canary_recovery_clears_consecutive(canary_env) -> None:
    dh, _ = canary_env
    toggles = iter([(502, b""), (502, b""), (200, b"")])

    def fake_toggle(url: str, timeout: float) -> tuple[int, bytes]:
        return next(toggles)

    dh.run_canary(_project(), http_get=fake_toggle)
    dh.run_canary(_project(), http_get=fake_toggle)
    dh.run_canary(_project(), http_get=fake_toggle)  # recovery

    assert not dh.is_tripped()
    # A subsequent failure starts the count at 1, not 3.
    dh.run_canary(_project(), http_get=lambda u, t: (502, b""))
    assert not dh.is_tripped()


def test_canary_connection_error_signature(canary_env) -> None:
    dh, _ = canary_env

    def fake_error(url: str, timeout: float) -> tuple[int, bytes]:
        raise urllib.error.URLError("connection refused")

    result = dh.run_canary(_project(), http_get=fake_error)
    assert not result.ok
    assert result.final_status is None
    assert result.signature() == "error:URLError"
    assert result.hops[0].error and "URLError" in result.hops[0].error


def test_log_rotation_triggers_at_threshold(canary_env) -> None:
    dh, data_dir = canary_env
    log_path = data_dir / "logs" / "deploy-health.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("x" * 1024, encoding="utf-8")

    dh.run_canary(
        _project(),
        http_get=lambda u, t: (200, b""),
        log_path=log_path,
    )
    # Pass a tiny threshold by forcing rotation via internal helper.
    dh._rotate_log(log_path, max_bytes=10)
    rotated = log_path.with_suffix(log_path.suffix + ".1")
    assert rotated.exists()


def test_main_canary_cli(canary_env, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    dh, _ = canary_env
    project = _project()

    monkeypatch.setattr(dh, "find_project", lambda slug: project)
    monkeypatch.setattr(dh, "_default_http_get", lambda url, timeout: (200, b""))

    rc = dh.main(["--canary", "demo"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "canary project=demo" in out
    assert "hop#1" in out
    assert "result ok=True" in out


def test_main_is_tripped_and_reset(canary_env, capsys) -> None:
    dh, _ = canary_env

    # Not tripped → exit 1.
    rc = dh.main(["--is-tripped"])
    assert rc == 1
    assert "ok" in capsys.readouterr().out

    # Trip manually and re-check.
    dh.TRIP_FLAG_PATH.parent.mkdir(parents=True, exist_ok=True)
    dh.TRIP_FLAG_PATH.write_text("{}", encoding="utf-8")
    rc = dh.main(["--is-tripped"])
    assert rc == 0
    assert "tripped" in capsys.readouterr().out

    rc = dh.main(["--reset"])
    assert rc == 0
    assert "cleared" in capsys.readouterr().out
    assert not dh.is_tripped()


def test_main_unknown_project(canary_env, capsys) -> None:
    dh, _ = canary_env

    from operator_core.config import ConfigError

    def _raise(_slug: str) -> ProjectConfig:
        raise ConfigError("Unknown project: ghost")

    import operator_core.deploy_health as target

    target.find_project = _raise  # type: ignore[assignment]
    rc = dh.main(["--canary", "ghost"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "Unknown project" in err


def test_main_help_when_no_args(canary_env, capsys) -> None:
    dh, _ = canary_env
    rc = dh.main([])
    assert rc == 2
    out = capsys.readouterr().out
    assert "canary" in out.lower()


@pytest.mark.skipif(
    os.environ.get("OPERATOR_SMOKE_LIVE") != "1",
    reason="live canary requires OPERATOR_SMOKE_LIVE=1",
)
def test_canary_live_integration() -> None:  # pragma: no cover - live only
    from operator_core import deploy_health as dh
    from operator_core.config import load_projects

    projects = load_projects()
    assert projects, "no projects registered"
    result = dh.run_canary(projects[0])
    assert result.hops
