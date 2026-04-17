"""Tests for C3 post-job reflections JSONL."""

from __future__ import annotations

import json

import pytest

from operator_core import runner as runner_module
from operator_core.runner import JobRunner, _files_touched_for_job, _write_reflection
from operator_core.store import JobStore


@pytest.fixture
def store(tmp_path):
    return JobStore(tmp_path / "jobs.sqlite3")


def test_write_reflection_skips_failed_jobs(tmp_path, store):
    job = store.create_job("status", prompt="hi")
    job = store.update_job(job.id, status="failed", metadata={"error": "boom"})
    log = tmp_path / "reflections.jsonl"
    result = _write_reflection(job, duration_sec=1.5, log_path=log)
    assert result is None
    assert not log.exists()


def test_write_reflection_appends_jsonl_line(tmp_path, store):
    job = store.create_job("build", prompt="Add darkmode", project="demo")
    job = store.update_job(
        job.id,
        status="complete",
        cost_usd=0.42,
        metadata={"reviewer_verdict": "PASS", "changed_files": ["src/a.ts", "src/b.ts"]},
    )
    log = tmp_path / "reflections.jsonl"
    rec = _write_reflection(job, duration_sec=12.75, log_path=log)
    assert rec is not None
    assert log.exists()
    lines = log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["project"] == "demo"
    assert parsed["command"] == "build"
    assert parsed["files_touched"] == ["src/a.ts", "src/b.ts"]
    assert parsed["duration_sec"] == 12.75
    assert parsed["cost_usd"] == 0.42
    assert parsed["reviewer_verdict"] == "PASS"
    assert parsed["status"] == "complete"
    assert "ts" in parsed
    assert "job_id" in parsed


def test_write_reflection_multiple_appends(tmp_path, store):
    job1 = store.create_job("status")
    job1 = store.update_job(job1.id, status="complete", metadata={})
    job2 = store.create_job("status")
    job2 = store.update_job(job2.id, status="complete", metadata={})
    log = tmp_path / "reflections.jsonl"
    _write_reflection(job1, duration_sec=1.0, log_path=log)
    _write_reflection(job2, duration_sec=2.0, log_path=log)
    lines = log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2


def test_files_touched_from_metadata_preferred(store):
    job = store.create_job("build", prompt="x", project="demo")
    job = store.update_job(
        job.id,
        status="complete",
        metadata={"changed_files": ["a.py", "b.py"]},
    )
    assert _files_touched_for_job(job) == ["a.py", "b.py"]


def test_files_touched_empty_for_non_git_job(store):
    job = store.create_job("status", prompt="hi")
    job = store.update_job(job.id, status="complete", metadata={})
    assert _files_touched_for_job(job) == []


def test_files_touched_empty_when_worktree_missing(store, tmp_path):
    job = store.create_job("build", prompt="x", project="demo")
    job = store.update_job(
        job.id,
        status="complete",
        worktree=str(tmp_path / "nonexistent"),
        metadata={},
    )
    assert _files_touched_for_job(job) == []


def test_needs_manual_status_still_reflects(tmp_path, store):
    job = store.create_job("build", prompt="x", project="demo")
    job = store.update_job(
        job.id,
        status="needs_manual",
        metadata={"reviewer_verdict": "PASS", "changed_files": []},
    )
    log = tmp_path / "reflections.jsonl"
    rec = _write_reflection(job, duration_sec=3.0, log_path=log)
    assert rec is not None
    assert log.exists()


def test_run_integration_writes_reflection(tmp_path, store, monkeypatch):
    """End-to-end: JobRunner.run() on a successful job writes one reflection line."""
    log = tmp_path / "reflections.jsonl"
    monkeypatch.setattr(runner_module, "REFLECTIONS_LOG_PATH", log)

    runner = JobRunner(store=store)
    job = store.create_job("status", prompt="ping")
    result = runner.run(job.id)
    assert result.status == "complete"
    assert log.exists()
    lines = log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["command"] == "status"
    assert parsed["status"] == "complete"


def test_run_integration_no_reflection_on_failure(tmp_path, store, monkeypatch):
    log = tmp_path / "reflections.jsonl"
    monkeypatch.setattr(runner_module, "REFLECTIONS_LOG_PATH", log)

    class _FailingRunner(JobRunner):
        def _run_once(self, job_id):
            return self.store.update_job(
                job_id,
                status="failed",
                metadata={"error": "unknown totally novel failure mode"},
            )

        def _sleep(self, seconds):
            pass

    runner = _FailingRunner(store=store)
    job = store.create_job("status", prompt="ping")
    result = runner.run(job.id)
    assert result.status == "failed"
    # Reflection only on success
    assert not log.exists()
