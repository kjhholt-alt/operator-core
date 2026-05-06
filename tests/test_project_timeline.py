from __future__ import annotations

import json

from operator_core.project_timeline import (
    collect_project_timelines,
    event_packet_context,
    find_timeline_event,
    project_timeline_dir,
    recommended_packet_kind,
)


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
        "jobs": [
            {
                "id": "job-1",
                "action": "agent.launch_prepared",
                "status": "queued",
                "project": "operator-core",
                "updated_at": "2026-05-06T12:10:00Z",
                "metadata": {"packet_id": "packet-1"},
            }
        ],
        "hook_blocks": [
            {
                "ts": "2026-05-06T12:11:00Z",
                "project": "operator-core",
                "tool_name": "Bash",
                "reason": "blocked dangerous command",
                "command": "rm -rf x",
            }
        ],
        "git_commits": {
            "operator-core": [
                {
                    "sha": "abcdef123456",
                    "ts": "2026-05-06T12:12:00Z",
                    "author": "Codex",
                    "subject": "Add timeline",
                    "path": str(tmp_path),
                }
            ]
        },
        "agent_launches": {
            "items": [
                {
                    "id": "launch-1",
                    "packet_id": "packet-1",
                    "packet_title": "Review autonomous merge",
                    "project": "operator-core",
                    "status": "prepared",
                    "updated_at": "2026-05-06T12:13:00Z",
                    "job_id": "job-1",
                    "paths": {"markdown": str(tmp_path / "launch.md")},
                }
            ]
        },
    }

    result = collect_project_timelines(state=state, output_dir=tmp_path / "timelines", write=True)

    assert result["summary"]["event_count"] >= 8
    assert result["summary"]["risk_count"] >= 4
    assert result["summary"]["actionable_count"] >= 4
    assert result["summary"]["counts_by_type"]["pr_merged_no_review"] == 1
    assert result["summary"]["counts_by_type"]["job_event"] == 1
    assert result["summary"]["counts_by_type"]["hook_block"] == 1
    assert result["summary"]["counts_by_type"]["local_commit"] == 1
    assert result["summary"]["counts_by_type"]["launch_prepared"] == 1
    assert "operator-core" in result["by_project"]
    assert any(event["type"] == "action_packet" for event in result["by_project"]["operator-core"])
    pr_event = next(event for event in result["by_project"]["operator-core"] if event["type"] == "pr_merged_no_review")
    assert pr_event["actionable"] is True
    assert pr_event["recommended_packet_kind"] == "weekly_review_follow_up"
    assert find_timeline_event(result, pr_event["id"]) == pr_event

    context = event_packet_context(pr_event, state=state)
    assert context["project"] == "operator-core"
    assert context["source_event"]["id"] == pr_event["id"]
    assert context["recommended_packet_kind"] == "weekly_review_follow_up"

    written = tmp_path / "timelines" / "operator-core.jsonl"
    assert written.exists()
    rows = [json.loads(line) for line in written.read_text(encoding="utf-8").splitlines()]
    assert rows
    assert all(row["project"] == "operator-core" for row in rows)


def test_project_timeline_dir_honors_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_PROJECT_TIMELINE_DIR", str(tmp_path / "custom"))

    assert project_timeline_dir(tmp_path) == tmp_path / "custom"


def test_recommended_packet_kind_rules():
    assert recommended_packet_kind({"type": "pr_merged_no_review", "severity": "low"}) == "weekly_review_follow_up"
    assert recommended_packet_kind({"type": "source_gap", "severity": "low"}) == "source_action_work_order"
    assert recommended_packet_kind({"type": "status_snapshot", "severity": "high"}) == "codex_implementation_packet"
    assert recommended_packet_kind({"type": "cost_rollup", "severity": "warn"}) == "codex_implementation_packet"
    assert recommended_packet_kind({"type": "agent_checkpoint", "severity": "warn"}) == "autonomy_checkpoint_draft"
    assert recommended_packet_kind({"type": "hook_block", "severity": "high"}) == "codex_implementation_packet"
    assert recommended_packet_kind({"type": "action_packet", "severity": "warn"}) == ""
