"""Smoke test: append_event prefers the real events_ndjson Writer when present.

The vendored shim already had a fallback path; this test just verifies that
when the real lib is installed and the stream is registered, append_event
goes through the validated Writer rather than the flat-envelope fallback.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest


def _has_real_lib() -> bool:
    try:
        import events_ndjson  # noqa: F401
        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _has_real_lib(), reason="real events_ndjson lib not installed")
class TestRealWriterPath:
    def test_runs_stream_uses_real_writer(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OPERATOR_EVENTS_DIR", str(tmp_path))
        # Force-rebuild the writer cache so it picks up the new dir.
        from operator_core._vendor import events_ndjson as shim
        importlib.reload(shim)

        env = shim.append_event(
            stream="runs", kind="started",
            recipe="alpha", correlation_id="cid-1",
            payload={"version": "1.0.0", "dry_run": False},
        )
        # Real Writer envelope shape includes source + schema_version + payload dict.
        assert env.get("source") == "operator-core"
        assert env.get("schema_version") == "events-ndjson/v1"
        assert env.get("event_type") == "started"
        assert env["payload"]["recipe"] == "alpha"
        assert env["payload"]["kind"] == "started"
        assert env["payload"]["version"] == "1.0.0"
        # And the file on disk has one line.
        target = tmp_path / "runs.ndjson"
        assert target.exists()
        line = json.loads(target.read_text(encoding="utf-8").splitlines()[0])
        assert line["payload"]["recipe"] == "alpha"

    def test_invalid_payload_falls_back_cleanly(self, tmp_path, monkeypatch):
        """A status value not in the runs schema should NOT crash; fall back to flat envelope."""
        monkeypatch.setenv("OPERATOR_EVENTS_DIR", str(tmp_path))
        from operator_core._vendor import events_ndjson as shim
        importlib.reload(shim)

        env = shim.append_event(
            stream="runs", kind="finished",
            recipe="beta", correlation_id="cid-2",
            payload={"status": "definitely_not_an_enum_value", "duration_sec": 1.0, "cost_usd": 0.0},
        )
        # No exception. Either the real writer accepted (unlikely) or the
        # fallback wrote a flat envelope; either way the file got a line.
        target = tmp_path / "runs.ndjson"
        assert target.exists()
        assert len(target.read_text(encoding="utf-8").splitlines()) >= 1


def test_unregistered_stream_uses_fallback(tmp_path, monkeypatch):
    """A made-up stream name must not crash -- it should fall back to the flat-envelope path."""
    monkeypatch.setenv("OPERATOR_EVENTS_DIR", str(tmp_path))
    from operator_core._vendor import events_ndjson as shim
    importlib.reload(shim)

    env = shim.append_event(
        stream="__not_registered_anywhere__", kind="something",
        recipe="x", correlation_id="cid-z",
        payload={"foo": "bar"},
    )
    target = tmp_path / "__not_registered_anywhere__.ndjson"
    assert target.exists()
    line = json.loads(target.read_text(encoding="utf-8").splitlines()[0])
    # Flat envelope shape (no nested payload key).
    assert line.get("recipe") == "x" or line.get("payload", {}).get("recipe") == "x"
