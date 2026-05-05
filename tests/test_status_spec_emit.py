"""Tests for the status-spec/v1 sibling emit."""
from __future__ import annotations

import json
import re
from pathlib import Path

from operator_core.utils import status_spec_emit


_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]00:00)$"
)


def test_translate_minimum_legacy():
    legacy = {"schema_version": 2, "last_updated": "2026-05-04T22:31:00.123456"}
    doc = status_spec_emit.translate(legacy)
    assert doc["schema_version"] == "status-spec/v1"
    assert doc["project"] == "operator-core"
    assert doc["health"] == "green"
    assert _TS_RE.match(doc["ts"])


def test_translate_marks_red_on_risk_tripped():
    legacy = {"schema_version": 2, "risk_tripped": True}
    doc = status_spec_emit.translate(legacy)
    assert doc["health"] == "red"


def test_translate_marks_yellow_on_hook_blocks():
    legacy = {
        "schema_version": 2,
        "hook_blocks_recent": [{"ts": "x", "reason": "y", "command": "z"}],
    }
    doc = status_spec_emit.translate(legacy)
    assert doc["health"] == "yellow"


def test_translate_marks_red_on_tripped_deploy():
    legacy = {"schema_version": 2, "deploy_health": {"a": "tripped"}}
    doc = status_spec_emit.translate(legacy)
    assert doc["health"] == "red"


def test_translate_subsystems_include_deploys_and_daemon():
    legacy = {
        "schema_version": 2,
        "deploy_health": {"a": "ok", "b": "warn"},
        "daemon": {"pid": 1234, "uptime_sec": 100},
    }
    doc = status_spec_emit.translate(legacy)
    names = {s["name"] for s in doc["subsystems"]}
    assert "deploy.a" in names
    assert "deploy.b" in names
    assert "daemon" in names


def test_translate_counters_are_numeric():
    legacy = {
        "schema_version": 2,
        "cost_today_usd": 1.23,
        "jobs_recent": [{"id": 1}, {"id": 2}],
        "discord_unread": 4,
    }
    doc = status_spec_emit.translate(legacy)
    counters = doc["counters"]
    assert counters["cost_today_usd"] == 1.23
    assert counters["jobs_recent"] == 2.0
    assert counters["discord_unread"] == 4.0


def test_emit_alongside_writes_atomically(tmp_path: Path):
    legacy_path = tmp_path / "status.json"
    legacy = {"schema_version": 2, "last_updated": "2026-05-04T22:31:00.123456"}
    out = status_spec_emit.emit_alongside(legacy, legacy_path)
    assert out == legacy_path.parent / "status-spec.json"
    assert out.exists()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["schema_version"] == "status-spec/v1"
    assert data["project"] == "operator-core"
    # No leftover .tmp files in the directory.
    leftovers = [f for f in tmp_path.iterdir() if f.name.endswith(".tmp")]
    assert leftovers == []


def test_status_write_emits_sibling(tmp_path: Path, monkeypatch):
    """End-to-end: writing the legacy status file should emit the sibling."""
    from operator_core.utils import status as status_mod

    target = tmp_path / "status.json"
    status_mod.write_status("test_section", {"value": 42}, path=target)

    sibling = tmp_path / "status-spec.json"
    assert sibling.exists()
    data = json.loads(sibling.read_text(encoding="utf-8"))
    assert data["schema_version"] == "status-spec/v1"
    assert data["project"] == "operator-core"
    assert data["health"] in ("green", "yellow", "red")
