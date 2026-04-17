from datetime import datetime

from operator_core.http_server import serve_http
from operator_core.scheduler import MorningOpsScheduler, ScheduledTask
from operator_core.store import JobStore


class DummyRunner:
    def __init__(self):
        self.ran = []

    def run(self, job_id):
        self.ran.append(job_id)


def test_http_server_refuses_public_bind(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    try:
        serve_http(store, host="0.0.0.0", port=8765)
    except ValueError as exc:
        assert "localhost" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_scheduler_catches_up_once(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    runner = DummyRunner()
    state_path = tmp_path / "scheduler.json"
    scheduler = MorningOpsScheduler(
        store,
        runner,
        tasks=[ScheduledTask("demo", "morning", "06:00")],
        state_path=state_path,
    )

    first = scheduler.tick(datetime(2026, 4, 11, 7, 0))
    second = scheduler.tick(datetime(2026, 4, 11, 8, 0))

    assert len(first) == 1
    assert second == []
