"""Tests for C2 smart retry logic in operator_core.runner."""

from __future__ import annotations

import json
import sqlite3

import pytest

from operator_core import runner as runner_module
from operator_core.runner import (
    ERROR_POLICIES,
    JobRunner,
    RetryPolicy,
    _retry_delay,
    classify_error,
)
from operator_core.store import JobStore
from operator_core.store_migrations import apply_migrations


# -- classify_error unit tests -------------------------------------------


def test_classify_rate_limit():
    p = classify_error("429 rate limit exceeded")
    assert p is not None
    assert p.name == "rate_limit"
    assert p.max_attempts == 4
    assert p.backoff == "exp"


def test_classify_worktree_lock():
    p = classify_error("Project demo is locked by another job: /tmp/demo.lock")
    assert p is not None
    assert p.name == "worktree_lock"
    assert p.backoff == "const"


def test_classify_git_push_race():
    p = classify_error("! [rejected] main -> main (non-fast-forward)")
    assert p is not None
    assert p.name == "git_push_race"


def test_classify_claude_5xx():
    p = classify_error("Claude 503 service unavailable")
    assert p is not None
    assert p.name == "claude_5xx"


def test_classify_transient_network():
    assert classify_error("Connection reset by peer").name == "transient_network"
    assert classify_error("Operation timed out").name == "transient_network"


def test_classify_hook_blocked_never_retries():
    assert classify_error("hook_blocked: rm -rf") is None
    assert classify_error("Blocked destructive command: git reset --hard") is None


def test_classify_risk_denied_never_retries():
    assert classify_error("risk gate denied: high-risk path") is None
    assert classify_error("auto-merge blocked — medium-risk needs 2 approvals") is None
    assert classify_error("High-risk changes require manual approval") is None


def test_classify_unknown_returns_none():
    assert classify_error("totally novel error message") is None
    assert classify_error("") is None


def test_retry_delay_exp_backoff():
    policy = RetryPolicy("x", max_attempts=4, retry_after_sec=2.0, backoff="exp")
    assert _retry_delay(policy, 0) == 2.0
    assert _retry_delay(policy, 1) == 4.0
    assert _retry_delay(policy, 2) == 8.0


def test_retry_delay_const_backoff():
    policy = RetryPolicy("x", max_attempts=3, retry_after_sec=5.0, backoff="const")
    assert _retry_delay(policy, 0) == 5.0
    assert _retry_delay(policy, 1) == 5.0


# -- JobRunner retry loop tests ------------------------------------------


@pytest.fixture
def store(tmp_path):
    return JobStore(tmp_path / "jobs.sqlite3")


class _FakeRunner(JobRunner):
    """JobRunner stub that injects a scripted sequence of errors into _run_once."""

    def __init__(self, store, errors):
        super().__init__(store=store)
        self._errors = list(errors)
        self.call_count = 0
        self.sleeps: list[float] = []

    def _sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)

    def _run_once(self, job_id):  # type: ignore[override]
        self.call_count += 1
        if not self._errors:
            return self.store.update_job(job_id, status="complete", metadata={"ok": True})
        err = self._errors.pop(0)
        if err is None:
            return self.store.update_job(job_id, status="complete", metadata={"ok": True})
        return self.store.update_job(job_id, status="failed", metadata={"error": err})


def test_retry_transient_then_success(store):
    job = store.create_job("status", prompt="hi")
    runner = _FakeRunner(store, errors=["Connection reset by peer", None])
    result = runner.run(job.id)
    assert result.status == "complete"
    assert runner.call_count == 2
    attempts = result.metadata["attempts"]
    assert len(attempts) == 2
    assert attempts[0]["status"] == "failed"
    assert attempts[1]["status"] == "complete"


def test_retry_exhausts_max_attempts(store):
    job = store.create_job("status", prompt="hi")
    runner = _FakeRunner(store, errors=["429 rate limit"] * 10)
    result = runner.run(job.id)
    assert result.status == "failed"
    assert runner.call_count == 4
    attempts = result.metadata["attempts"]
    assert len(attempts) == 4
    assert "exhausted" in attempts[-1]["retry_decision"]


def test_hook_blocked_never_retries(store):
    job = store.create_job("status", prompt="hi")
    runner = _FakeRunner(store, errors=["hook_blocked: destructive"])
    result = runner.run(job.id)
    assert result.status == "failed"
    assert runner.call_count == 1
    attempts = result.metadata["attempts"]
    assert len(attempts) == 1
    assert "no_policy" in attempts[0]["retry_decision"]


def test_risk_denied_never_retries(store):
    job = store.create_job("status", prompt="hi")
    runner = _FakeRunner(store, errors=["risk gate denied: high-risk path"])
    result = runner.run(job.id)
    assert result.status == "failed"
    assert runner.call_count == 1


def test_unknown_error_not_retried(store):
    job = store.create_job("status", prompt="hi")
    runner = _FakeRunner(store, errors=["something completely unknown"])
    result = runner.run(job.id)
    assert result.status == "failed"
    assert runner.call_count == 1


def test_git_push_race_retries_then_succeeds(store):
    job = store.create_job("status", prompt="hi")
    runner = _FakeRunner(store, errors=["! [rejected] non-fast-forward", None])
    result = runner.run(job.id)
    assert result.status == "complete"
    assert runner.call_count == 2


def test_worktree_lock_const_backoff(store):
    job = store.create_job("status", prompt="hi")
    runner = _FakeRunner(store, errors=["Project demo is locked by another job"] * 5)
    result = runner.run(job.id)
    assert runner.call_count == 3
    assert runner.sleeps == [5.0, 5.0]
    assert result.status == "failed"


def test_attempts_persisted_to_jobs_row(store):
    job = store.create_job("status", prompt="hi")
    runner = _FakeRunner(store, errors=["429 rate limit", None])
    runner.run(job.id)
    conn = sqlite3.connect(store.db_path)
    try:
        apply_migrations(conn)
        row = conn.execute("SELECT attempts_json FROM jobs WHERE id = ?", (job.id,)).fetchone()
    finally:
        conn.close()
    assert row is not None
    persisted = json.loads(row[0])
    assert len(persisted) == 2
    assert persisted[0]["status"] == "failed"
    assert persisted[1]["status"] == "complete"


def test_success_on_first_try_single_attempt(store):
    job = store.create_job("status", prompt="hi")
    runner = _FakeRunner(store, errors=[])
    result = runner.run(job.id)
    assert result.status == "complete"
    assert runner.call_count == 1
    assert len(result.metadata["attempts"]) == 1


def test_error_policies_table_is_ordered_and_non_empty():
    assert len(ERROR_POLICIES) >= 5
    names = {p.name for _, p in ERROR_POLICIES}
    assert {"rate_limit", "worktree_lock", "git_push_race", "claude_5xx", "transient_network"} <= names
