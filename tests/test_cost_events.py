"""Cost-stream NDJSON emitter tests."""

import json
import os

import pytest

from operator_core import cost_events
from operator_core._vendor.events_ndjson import EventsNdjsonError, Writer


@pytest.fixture(autouse=True)
def _reset_writer(monkeypatch):
    cost_events.reset_for_tests()
    yield
    cost_events.reset_for_tests()
    monkeypatch.delenv("OPERATOR_COST_NDJSON_PATH", raising=False)


def test_emit_cost_no_op_when_unset(monkeypatch):
    monkeypatch.delenv("OPERATOR_COST_NDJSON_PATH", raising=False)
    cost_events.reset_for_tests()
    assert cost_events.emit_cost(agent="x", cost_usd=0.5) is False


def test_emit_cost_writes_envelope(tmp_path, monkeypatch):
    p = tmp_path / "costs.ndjson"
    monkeypatch.setenv("OPERATOR_COST_NDJSON_PATH", str(p))
    cost_events.reset_for_tests()

    ok = cost_events.emit_cost(
        agent="morning-briefing",
        cost_usd=0.42,
        duration_ms=1234,
        model="claude-opus",
    )
    assert ok is True
    assert p.exists()
    line = p.read_text(encoding="utf-8").strip()
    obj = json.loads(line)
    assert obj["stream"] == "cost"
    assert obj["source"] == "operator-core"
    assert obj["event_type"] == "agent_complete"
    assert obj["payload"]["agent"] == "morning-briefing"
    assert obj["payload"]["cost_usd"] == 0.42
    assert obj["payload"]["duration_ms"] == 1234
    assert obj["payload"]["model"] == "claude-opus"
    assert obj["schema_version"] == "events-ndjson/v1"


def test_emit_cost_appends_in_order(tmp_path, monkeypatch):
    p = tmp_path / "costs.ndjson"
    monkeypatch.setenv("OPERATOR_COST_NDJSON_PATH", str(p))
    cost_events.reset_for_tests()
    for i in range(5):
        cost_events.emit_cost(agent=f"a{i}", cost_usd=float(i))
    lines = p.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 5
    for i, line in enumerate(lines):
        assert json.loads(line)["payload"]["agent"] == f"a{i}"


def test_writer_validates_payload(tmp_path):
    p = tmp_path / "costs.ndjson"
    w = Writer(stream="cost", source="test", path=p)
    with pytest.raises(EventsNdjsonError):
        w.append("bad", {"missing": "agent"})
    w.close()


def test_writer_rejects_unsupported_stream(tmp_path):
    with pytest.raises(EventsNdjsonError):
        Writer(stream="pacing", source="test", path=tmp_path / "x.ndjson")


def test_writer_atomic_under_threads(tmp_path):
    """Concurrent appenders on the vendored stub keep lines intact."""
    import threading

    p = tmp_path / "costs.ndjson"
    w = Writer(stream="cost", source="test", path=p)
    barrier = threading.Barrier(4)

    def worker(idx: int):
        barrier.wait()
        for j in range(25):
            w.append("agent_complete", {"agent": f"t{idx}-{j}", "cost_usd": float(j)})

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    w.close()

    lines = p.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 100
    for line in lines:
        # Each line must be a complete JSON object.
        json.loads(line)
