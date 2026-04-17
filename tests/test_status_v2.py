"""Tests for the v2 `.operator-status.json` schema + migration path."""
from __future__ import annotations

import json

from operator_core.utils import status as status_mod


def test_load_or_default_returns_fresh_v2_when_missing(tmp_path):
    target = tmp_path / "status.json"
    data = status_mod.load_or_default(target)
    assert data["schema_version"] == 2
    assert data["daemon"] == {"pid": None, "started_at": None, "uptime_sec": 0}
    assert data["jobs_recent"] == []
    assert data["cost_today_usd"] == 0.0
    assert data["deploy_health"] == {}
    assert data["risk_tripped"] is False
    assert data["hook_blocks_recent"] == []
    assert data["discord_unread"] == 0


def test_load_or_default_migrates_legacy_v1_without_clobbering(tmp_path):
    target = tmp_path / "status.json"
    legacy = {
        "morning-briefing": {"timestamp": "2026-04-10T06:00:00", "ok": True},
        "_alerts": {"vercel": {"state": "ok"}},
        "last_updated": "2026-04-10T06:00:00",
    }
    target.write_text(json.dumps(legacy), encoding="utf-8")
    data = status_mod.load_or_default(target)
    # legacy keys preserved
    assert data["morning-briefing"] == legacy["morning-briefing"]
    assert data["_alerts"] == legacy["_alerts"]
    # v2 defaults added
    assert data["schema_version"] == 2
    assert data["jobs_recent"] == []
    assert data["cost_today_usd"] == 0.0


def test_load_or_default_handles_corrupt_file(tmp_path):
    target = tmp_path / "status.json"
    target.write_text("{not json", encoding="utf-8")
    data = status_mod.load_or_default(target)
    assert data["schema_version"] == 2


def test_update_daemon_writes_fields(tmp_path):
    target = tmp_path / "status.json"
    status_mod.update_daemon(4242, "2026-04-11T06:00:00", 120.5, target)
    data = status_mod.load_or_default(target)
    assert data["daemon"]["pid"] == 4242
    assert data["daemon"]["started_at"] == "2026-04-11T06:00:00"
    assert data["daemon"]["uptime_sec"] == 120.5
    assert data["last_updated"] is not None


def test_record_recent_job_trims_to_ten(tmp_path):
    target = tmp_path / "status.json"
    for i in range(15):
        status_mod.record_recent_job(
            {
                "id": f"job{i:02d}",
                "action": "morning",
                "status": "done",
                "project": "operator-ai",
                "cost_usd": 0.01 * i,
                "updated_at": f"2026-04-11T06:{i:02d}:00",
            },
            target,
        )
    data = status_mod.load_or_default(target)
    assert len(data["jobs_recent"]) == 10
    # most recent first
    assert data["jobs_recent"][0]["id"] == "job14"
    assert data["jobs_recent"][-1]["id"] == "job05"


def test_record_recent_job_dedupes_by_id(tmp_path):
    target = tmp_path / "status.json"
    status_mod.record_recent_job({"id": "same", "action": "a", "status": "queued"}, target)
    status_mod.record_recent_job({"id": "same", "action": "a", "status": "done"}, target)
    data = status_mod.load_or_default(target)
    assert len(data["jobs_recent"]) == 1
    assert data["jobs_recent"][0]["status"] == "done"


def test_record_hook_block_keeps_last_five(tmp_path):
    target = tmp_path / "status.json"
    for i in range(8):
        status_mod.record_hook_block(f"reason{i}", f"rm -rf /tmp/{i}", target)
    data = status_mod.load_or_default(target)
    assert len(data["hook_blocks_recent"]) == 5
    assert data["hook_blocks_recent"][0]["reason"] == "reason7"


def test_set_cost_today_and_deploy_health_and_flags(tmp_path):
    target = tmp_path / "status.json"
    status_mod.set_cost_today(3.21, target)
    status_mod.set_deploy_health("operator-ai", "ok", target)
    status_mod.set_deploy_health("dealbrain", "warn", target)
    status_mod.set_risk_tripped(True, target)
    status_mod.set_discord_unread(4, target)
    data = status_mod.load_or_default(target)
    assert data["cost_today_usd"] == 3.21
    assert data["deploy_health"] == {"operator-ai": "ok", "dealbrain": "warn"}
    assert data["risk_tripped"] is True
    assert data["discord_unread"] == 4


def test_write_status_preserves_v2_keys(tmp_path):
    target = tmp_path / "status.json"
    status_mod.set_cost_today(1.50, target)
    status_mod.write_status("morning-briefing", {"ok": True}, target)
    data = status_mod.load_or_default(target)
    assert data["morning-briefing"]["ok"] is True
    assert data["cost_today_usd"] == 1.50
    assert data["schema_version"] == 2


def test_set_alert_state_migrates_and_preserves(tmp_path):
    target = tmp_path / "status.json"
    status_mod.set_alert_state("vercel", "down", "500s", target)
    data = status_mod.load_or_default(target)
    assert data["_alerts"]["vercel"]["state"] == "down"
    assert data["schema_version"] == 2
