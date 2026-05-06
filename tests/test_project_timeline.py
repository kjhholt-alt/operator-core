from __future__ import annotations

import json

from operator_core.project_timeline import collect_project_timelines, project_timeline_dir


def test_project_timeline_normalizes_existing_cockpit_facts(tmp_path):
    state = {
        "generated_at": "2026-05-06T12:00:00Z",
        "artifacts": {"portfolio_cost": {"path": str(tmp_path / "portfolio_cost.json")}},
        "statuses": {
            "items": [
                {
                    "project": "operator-core",
                    "health": "red",
                    "summary": "tests failing",
                    "ts": "2026-05-06T11:00:00Z",
                    "path": str(tmp_path / "status" / "operator-core.json"),
                }
            ]
        },
        "weekly_review": {
            "auto_merged": [
                {
                    "repo_short": "operator-core",
                    "number": 42,
                    "title": "large autonomous merge",
                    "additions": 600,
                    "deletions": 10,
                    "files": 12,
                    "merged_at": "2026-05-06T10:00:00Z",
                }
            ],
            "human_reviewed": [],
        },
        "cost": {
            "generated_at": "2026-05-06T12:00:00Z",
            "total_30d_usd": 3.5,
            "by_project": {"operator-core": 1.25},
            "by_recipe": {"weekly_review": 1.25},
        },
        "autonomy_evidence": {
            "latest": {
                "run_id": "run-1",
                "mission_title": "Operator Core cockpit",
                "mission_id": "cockpit",
                "phase": "verify",
                "checkpoints": [
                    {
                        "name": "checkpoint_001.md",
                        "time": "2026-05-06T11:30:00Z",
                        "status": "active",
                        "phase": "verify",
                        "summary": "Timeline connected.",
                        "path": str(tmp_path / "checkpoint_001.md"),
                    }
                ],
            }
        },
        "action_packets": {
            "items": [
                {
                    "id": "packet-1",
                    "kind": "weekly_review_follow_up",
                    "kind_label": "Create weekly review follow-up packet",
                    "title": "Review autonomous merge",
                    "status": "ready",
                    "updated_at": "2026-05-06T12:05:00Z",
                    "context": {"project": "operator-core"},
                    "paths": {"markdown": str(tmp_path / "packet-1.md")},
                }
            ]
        },
        "source_registry": {
            "items": [
                {
                    "id": "stale-source",
                    "label": "Stale source",
                    "connection": "connected",
                    "health": "stale",
                    "category": "review",
                    "file_count": 1,
                    "latest_updated_at": "2026-05-05T00:00:00Z",
                }
            ]
        },
        "memory_learning": {"decisions": [{"decision": "Cockpit is control surface", "why": "steering"}]},
        "portfolio_motion": {"project_motion": {"top_mover": {"title": "Operator Core", "motion_score": 90, "next_action": "Ship timeline"}}},
    }

    result = collect_project_timelines(state=state, output_dir=tmp_path / "timelines", write=True)

    assert result["summary"]["event_count"] >= 8
    assert result["summary"]["risk_count"] >= 4
    assert result["summary"]["counts_by_type"]["pr_merged_no_review"] == 1
    assert "operator-core" in result["by_project"]
    assert any(event["type"] == "action_packet" for event in result["by_project"]["operator-core"])

    written = tmp_path / "timelines" / "operator-core.jsonl"
    assert written.exists()
    rows = [json.loads(line) for line in written.read_text(encoding="utf-8").splitlines()]
    assert rows
    assert all(row["project"] == "operator-core" for row in rows)


def test_project_timeline_dir_honors_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_PROJECT_TIMELINE_DIR", str(tmp_path / "custom"))

    assert project_timeline_dir(tmp_path) == tmp_path / "custom"
