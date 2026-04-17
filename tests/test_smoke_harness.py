"""Tests for the Operator live smoke harness.

No network. The real `JobRunner` is replaced with a fake that just flips the
job status so we can assert the harness sequencing, JSONL logging, flag
parsing, and error handling paths.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from operator_core import smoke
from operator_core.smoke import SMOKE_SEQUENCE, SmokeReport, run_smoke
from operator_core.store import JobStore


class _FakeRunner:
    """Stand-in for `operator_core.runner.JobRunner` used by the smoke tests."""

    def __init__(
        self,
        store: JobStore,
        *,
        fail_actions: set[str] | None = None,
        raise_actions: set[str] | None = None,
    ) -> None:
        self.store = store
        self.fail_actions = fail_actions or set()
        self.raise_actions = raise_actions or set()
        self.calls: list[str] = []

    def run(self, job_id: str) -> Any:
        job = self.store.get_job(job_id)
        self.calls.append(job.action)
        if job.action in self.raise_actions:
            raise RuntimeError(f"boom-{job.action}")
        status = "failed" if job.action in self.fail_actions else "complete"
        print(f"[fake] {job.action} -> {status}")
        return self.store.update_job(job_id, status=status)


@pytest.fixture()
def smoke_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect Operator data dir to a scratch path so the ledger is isolated."""
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

    import importlib
    from operator_core import settings as settings_mod
    settings_mod.clear_cache()

    from operator_core import paths as paths_module
    importlib.reload(paths_module)
    importlib.reload(smoke)
    from operator_core import store as store_module

    importlib.reload(store_module)
    return data_dir


def test_dry_run_happy_path_writes_jsonl(smoke_env: Path) -> None:
    from operator_core.store import JobStore as FreshStore

    store = FreshStore()
    fake = _FakeRunner(store)

    report = run_smoke(live=False, record=False, store=store, runner=fake)

    assert report.ok is True
    assert len(report.steps) == len(SMOKE_SEQUENCE)
    assert fake.calls == [
        "status",
        "morning",
        "review_prs",
        "deploy_check",
        "deck_ag_market_pulse",
    ]
    non_help = [step for step in report.steps if step.action != "help"]
    assert all(step.job_id for step in non_help)

    log_path = Path(report.log_path)
    assert log_path.exists()
    lines = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    assert lines[0]["type"] == "smoke_start"
    assert lines[-1]["type"] == "smoke_end"
    assert lines[-1]["ok"] is True
    assert lines[-1]["steps"] == len(SMOKE_SEQUENCE)
    step_lines = [line for line in lines if line["type"] == "smoke_step"]
    assert len(step_lines) == len(SMOKE_SEQUENCE)
    assert all("stdout_bytes" in line for line in step_lines)
    assert all("stdout" not in line for line in step_lines)


def test_record_flag_persists_stdout(smoke_env: Path) -> None:
    from operator_core.store import JobStore as FreshStore

    store = FreshStore()
    fake = _FakeRunner(store)

    report = run_smoke(live=False, record=True, store=store, runner=fake)

    log_lines = [
        json.loads(line)
        for line in Path(report.log_path).read_text(encoding="utf-8").splitlines()
    ]
    step_lines = [line for line in log_lines if line["type"] == "smoke_step"]
    captured = [line for line in step_lines if "[fake]" in line.get("stdout", "")]
    assert captured, "record mode should retain captured stdout"
    help_step = next(line for line in step_lines if line["action"] == "help")
    assert "Operator V3 commands" in help_step["stdout"]


def test_failed_job_is_captured_but_harness_continues(smoke_env: Path) -> None:
    from operator_core.store import JobStore as FreshStore

    store = FreshStore()
    fake = _FakeRunner(store, fail_actions={"morning"})

    report = run_smoke(live=False, record=False, store=store, runner=fake)

    assert report.ok is False
    statuses = {step.action: step.status for step in report.steps}
    assert statuses["morning"] == "failed"
    assert statuses["deploy_check"] == "complete"
    failing = [step for step in report.steps if step.exit_code != 0]
    assert len(failing) == 1
    assert failing[0].action == "morning"


def test_runner_exception_becomes_error_step(smoke_env: Path) -> None:
    from operator_core.store import JobStore as FreshStore

    store = FreshStore()
    fake = _FakeRunner(store, raise_actions={"deploy_check"})

    report = run_smoke(live=False, record=False, store=store, runner=fake)

    assert report.ok is False
    bad = next(step for step in report.steps if step.action == "deploy_check")
    assert bad.status == "error"
    assert bad.exit_code == 1
    assert "boom-deploy_check" in (bad.error or "")


def test_dry_run_env_is_restored(smoke_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPERATOR_V3_DRY_RUN", raising=False)
    from operator_core.store import JobStore as FreshStore

    store = FreshStore()
    fake = _FakeRunner(store)

    run_smoke(live=False, record=False, store=store, runner=fake)

    assert "OPERATOR_V3_DRY_RUN" not in os.environ


def test_live_flag_skips_dry_run_toggle(smoke_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPERATOR_V3_DRY_RUN", raising=False)
    from operator_core.store import JobStore as FreshStore

    store = FreshStore()
    fake = _FakeRunner(store)

    run_smoke(live=True, record=False, store=store, runner=fake)

    assert "OPERATOR_V3_DRY_RUN" not in os.environ


def test_cli_main_flag_parsing(
    smoke_env: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    captured_kwargs: dict[str, Any] = {}

    def _fake_run_smoke(live: bool = False, record: bool = False, **_: Any) -> SmokeReport:
        captured_kwargs["live"] = live
        captured_kwargs["record"] = record
        return SmokeReport(
            started_at="2026-04-11T00:00:00",
            finished_at="2026-04-11T00:00:01",
            live=live,
            record=record,
            log_path=str(smoke_env / "logs" / "smoke-stub.jsonl"),
            steps=[],
        )

    monkeypatch.setattr(smoke, "run_smoke", _fake_run_smoke)

    rc = smoke.main(["--live", "--record"])
    assert rc == 0
    assert captured_kwargs == {"live": True, "record": True}
    out = capsys.readouterr().out
    assert "smoke log:" in out
    assert "overall: OK" in out


def test_cli_main_failing_report_returns_nonzero(
    smoke_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from operator_core.smoke import SmokeStepResult

    def _fake_run_smoke(live: bool = False, record: bool = False, **_: Any) -> SmokeReport:
        return SmokeReport(
            started_at="2026-04-11T00:00:00",
            finished_at="2026-04-11T00:00:01",
            live=live,
            record=record,
            log_path=str(smoke_env / "logs" / "smoke-fail.jsonl"),
            steps=[
                SmokeStepResult(
                    command="!op status",
                    action="status",
                    job_id="abc",
                    status="failed",
                    exit_code=1,
                    stdout="",
                )
            ],
        )

    monkeypatch.setattr(smoke, "run_smoke", _fake_run_smoke)
    rc = smoke.main([])
    assert rc == 1


def test_log_path_follows_timestamp(smoke_env: Path) -> None:
    from operator_core.store import JobStore as FreshStore

    store = FreshStore()
    fake = _FakeRunner(store)

    fixed = datetime(2026, 4, 11, 3, 30, 0)
    report = run_smoke(
        live=False,
        record=False,
        store=store,
        runner=fake,
        now=lambda: fixed,
    )
    assert Path(report.log_path).name == "smoke-20260411-0330.jsonl"
