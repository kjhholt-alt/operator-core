"""Tests for the status-spec / events-ndjson vendor shims.

The shims are designed to work in two modes:
  1. Real packages installed (`pip install operator-core[specs]`)
  2. Real packages absent — fallback to in-tree minimal implementations

These tests exercise both paths and assert the public contract holds
in both. They also lock in the behaviors that surprised us during the
initial wiring (lazy paths, canonical aggregate emission, status->health
mapping, schema-validated runs stream).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from operator_core._vendor import events_ndjson, status_spec


# ---------------------------------------------------------------------------
# Diagnostic: tests run in whichever mode the current install is in.
# ---------------------------------------------------------------------------


def test_using_real_lib_diagnostic_is_consistent():
    """If real events-ndjson is in use, real status-spec almost certainly is
    too (they're installed together via the [specs] extra). Soft check —
    the diagnostic just needs to be a bool."""
    assert isinstance(events_ndjson.using_real_lib(), bool)
    assert isinstance(status_spec.using_real_lib(), bool)


# ---------------------------------------------------------------------------
# events-ndjson: append_event / read_events (always present on both paths)
# ---------------------------------------------------------------------------


def test_append_event_and_read_back(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_EVENTS_DIR", str(tmp_path))
    # Reload so the writer cache picks up the env override.
    import importlib
    from operator_core._vendor import events_ndjson as _ev_reload
    importlib.reload(_ev_reload)

    _ev_reload.append_event(
        stream="runs",
        kind="started",
        recipe="test_recipe",
        correlation_id="c1",
        payload={"version": "1.0", "dry_run": False},
    )
    _ev_reload.append_event(
        stream="runs",
        kind="finished",
        recipe="test_recipe",
        correlation_id="c1",
        payload={"status": "ok", "duration_sec": 0.5, "cost_usd": 0.01},
    )

    events = _ev_reload.read_events("runs")
    assert len(events) == 2

    # Real Writer wraps all stream-specific fields inside `payload`; the
    # fallback flat-envelope path puts them at the top level. Read both.
    def _field(env, key):
        if key in env:
            return env[key]
        return (env.get("payload") or {}).get(key)

    assert _field(events[0], "kind") == "started"
    assert _field(events[0], "version") == "1.0"
    assert _field(events[1], "kind") == "finished"
    assert _field(events[1], "cost_usd") == 0.01


def test_append_event_rejects_empty_stream():
    with pytest.raises(events_ndjson.EventsNdjsonError):
        events_ndjson.append_event(stream="", kind="started")


def test_append_event_rejects_empty_kind():
    with pytest.raises(events_ndjson.EventsNdjsonError):
        events_ndjson.append_event(stream="runs", kind="")


def test_read_events_returns_empty_list_when_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_EVENTS_DIR", str(tmp_path))
    assert events_ndjson.read_events("never_written") == []


# ---------------------------------------------------------------------------
# events-ndjson: Writer (cost stream, both paths)
# ---------------------------------------------------------------------------


def test_writer_writes_cost_event(tmp_path):
    p = tmp_path / "cost.ndjson"
    w = events_ndjson.Writer(stream="cost", source="test", path=p)
    try:
        env = w.append("agent_complete", {"agent": "test", "cost_usd": 0.02})
    finally:
        w.close()

    line = p.read_text(encoding="utf-8").strip()
    parsed = json.loads(line)
    assert parsed["stream"] == "cost"
    assert parsed["payload"]["agent"] == "test"
    assert parsed["payload"]["cost_usd"] == 0.02
    # Real lib uses event_type at top-level OR stream-keyed; both shapes
    # carry the agent + cost in payload, which is what cost_tracker reads.


def test_writer_rejects_unknown_stream(tmp_path):
    """Both paths reject an unregistered stream name."""
    with pytest.raises(events_ndjson.EventsNdjsonError):
        events_ndjson.Writer(
            stream="__definitely_not_a_real_stream__",
            source="test",
            path=tmp_path / "x.ndjson",
        )


# ---------------------------------------------------------------------------
# status-spec: write_component_status legacy aggregate (both paths)
# ---------------------------------------------------------------------------


def test_legacy_aggregate_overall_ok(tmp_path):
    status_spec.write_component_status("a", "ok", status_dir=tmp_path)
    status_spec.write_component_status("b", "ok", status_dir=tmp_path)
    aggregate = json.loads((tmp_path.parent / "status.json").read_text(encoding="utf-8"))
    assert aggregate["overall"] == "ok"
    assert set(aggregate["components"]) == {"a", "b"}


def test_legacy_aggregate_overall_warn_on_warn(tmp_path):
    status_spec.write_component_status("a", "ok", status_dir=tmp_path)
    status_spec.write_component_status("b", "warn", status_dir=tmp_path, error="degraded")
    aggregate = json.loads((tmp_path.parent / "status.json").read_text(encoding="utf-8"))
    assert aggregate["overall"] == "warn"


def test_legacy_aggregate_overall_error_wins(tmp_path):
    status_spec.write_component_status("a", "warn", status_dir=tmp_path)
    status_spec.write_component_status("b", "error", status_dir=tmp_path, error="boom")
    status_spec.write_component_status("c", "ok", status_dir=tmp_path)
    aggregate = json.loads((tmp_path.parent / "status.json").read_text(encoding="utf-8"))
    assert aggregate["overall"] == "error"


def test_lazy_path_resolution_picks_up_env_change(tmp_path, monkeypatch):
    """Regression: AGGREGATE_PATH was computed at import time. Now lazy."""
    monkeypatch.setenv("OPERATOR_STATUS_DIR", str(tmp_path / "status"))
    status_spec.write_component_status("recipe_a", "ok")
    legacy = tmp_path / "status.json"
    assert legacy.exists(), "lazy path should write to env-overridden dir"


# ---------------------------------------------------------------------------
# status-spec: canonical aggregate (real lib only)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not status_spec.using_real_lib(),
    reason="canonical aggregate requires real status-spec package",
)
def test_canonical_aggregate_emits_status_spec_v1(tmp_path, monkeypatch):
    """The shim's _emit_canonical_aggregate_to() builds a valid status-spec/v1 doc.

    Note: write_component_status no longer auto-emits the canonical doc
    (see comment in shim — daemon owns that path). This test exercises
    the helper directly so the translation logic stays covered.
    """
    monkeypatch.setenv("OPERATOR_STATUS_DIR", str(tmp_path / "status"))
    status_spec.write_component_status("a", "ok", duration_sec=1.0, cost_usd=0.05)
    status_spec.write_component_status("b", "warn", duration_sec=2.0, error="slow")
    canonical = tmp_path / "canonical.json"
    components = {
        "a": {"name": "a", "status": "ok", "cost_usd": 0.05},
        "b": {"name": "b", "status": "warn", "cost_usd": 0.0, "error": "slow"},
    }
    status_spec._emit_canonical_aggregate_to(canonical, tmp_path / "status", components)
    doc = json.loads(canonical.read_text(encoding="utf-8"))
    assert doc["schema_version"] == "status-spec/v1"
    assert doc["health"] == "yellow"  # one warn -> yellow
    names = {s["name"] for s in doc["subsystems"]}
    assert names == {"a", "b"}
    counters = doc["counters"]
    assert counters["components_total"] == 2
    assert counters["components_errors"] == 0
    assert counters["cost_usd_sum"] == 0.05


@pytest.mark.skipif(
    not status_spec.using_real_lib(),
    reason="canonical aggregate requires real status-spec package",
)
def test_canonical_aggregate_health_red_on_error(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_STATUS_DIR", str(tmp_path / "status"))
    status_spec.write_component_status("a", "ok")
    status_spec.write_component_status("b", "error", error="boom")
    canonical = tmp_path / "canonical.json"
    components = {
        "a": {"name": "a", "status": "ok"},
        "b": {"name": "b", "status": "error", "error": "boom"},
    }
    status_spec._emit_canonical_aggregate_to(canonical, tmp_path / "status", components)
    doc = json.loads(canonical.read_text(encoding="utf-8"))
    assert doc["health"] == "red"
    assert doc["counters"]["components_errors"] == 1


@pytest.mark.skipif(
    not status_spec.using_real_lib(),
    reason="canonical aggregate requires real status-spec package",
)
def test_write_component_status_does_NOT_auto_emit_canonical(tmp_path, monkeypatch):
    """Regression: the daemon owns canonical emit. Recipes runtime must not race it."""
    monkeypatch.setenv("OPERATOR_STATUS_DIR", str(tmp_path / "status"))
    status_spec.write_component_status("recipe_a", "ok")
    canonical = tmp_path / "status-spec.json"
    assert not canonical.exists(), (
        "write_component_status should not auto-emit canonical "
        "(daemon owns that path; concurrent writes were racing)"
    )


# ---------------------------------------------------------------------------
# events-ndjson: runs schema validation (real lib only)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not events_ndjson.using_real_lib(),
    reason="runs schema validation requires real events-ndjson",
)
def test_real_lib_validates_runs_kind_enum(tmp_path):
    """The runs schema restricts `kind` to a known enum. Verify that
    a bogus kind would fail validation when constructed via the real
    library's Writer (we use append_event which doesn't validate, but
    this catches an invariant we rely on)."""
    from events_ndjson._validate import validate_stream_payload

    # valid started shape
    validate_stream_payload("runs", {"recipe": "x", "kind": "started"})

    # invalid kind: real lib raises StreamError on schema-violation payloads
    with pytest.raises(events_ndjson.EventsNdjsonError):
        validate_stream_payload("runs", {"recipe": "x", "kind": "bogus"})

    # missing required recipe
    with pytest.raises(events_ndjson.EventsNdjsonError):
        validate_stream_payload("runs", {"kind": "started"})
