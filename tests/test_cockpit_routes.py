from __future__ import annotations

import http.client
import json
import os
import socket
import threading
import time
from pathlib import Path

import pytest

from operator_core.cockpit_routes import collect_cockpit_state, register_cockpit_routes
from operator_core.http_server import EXTRA_ROUTES, OperatorHttpServer
from operator_core.store import JobStore
from operator_core.war_room_agents import collect_agent_coordination
from operator_core.war_room_autonomy import collect_autonomy_evidence
from operator_core.war_room_memory import collect_memory_learning
from operator_core.war_room_mission import collect_mission_control
from operator_core.war_room_portfolio_motion import collect_portfolio_motion
from operator_core.war_room_quality import collect_quality_history
from operator_core.war_room_sources import collect_source_registry


@pytest.fixture
def cockpit_env(tmp_path, monkeypatch):
    data = tmp_path / "data"
    status = data / "status"
    war = tmp_path / "war-room"
    status.mkdir(parents=True)
    war.mkdir()
    monkeypatch.setenv("OPERATOR_DATA_DIR", str(data))
    monkeypatch.setenv("OPERATOR_STATUS_DIR", str(status))
    monkeypatch.setenv("OPERATOR_WAR_ROOM_DIR", str(war))
    monkeypatch.setenv("OPERATOR_PORTFOLIO_COST_PATH", str(data / "portfolio_cost.json"))

    (status / "operator-core.json").write_text(json.dumps({
        "schema_version": "status-spec/v1",
        "project": "operator-core",
        "ts": "2026-05-06T12:00:00Z",
        "health": "green",
        "summary": "ok",
    }), encoding="utf-8")
    (status / "pl-engine-dashboard.json").write_text(json.dumps({
        "schema_version": "status-spec/v1",
        "project": "pl-engine-dashboard",
        "ts": "2026-05-06T12:00:00Z",
        "health": "red",
        "summary": "refresh failed",
    }), encoding="utf-8")
    (war / "portfolio-health.ir.json").write_text(json.dumps({
        "title": "Portfolio Health",
        "subtitle": "sample",
        "sections": [{
            "title": "Overview",
            "components": [
                {"type": "kpi_tile", "label": "Tracked", "value": 2},
                {"type": "kpi_tile", "label": "Green", "value": 1},
                {"type": "kpi_tile", "label": "Red", "value": 1},
            ],
        }],
    }), encoding="utf-8")
    (war / "morning.md").write_text("**Overnight:** sample\n- shipped cockpit\n", encoding="utf-8")
    (war / "weekly-review.json").write_text(json.dumps({
        "total": 1,
        "auto_merged": [{
            "repo_short": "operator-core",
            "number": 8,
            "title": "sample no-review merge",
            "additions": 100,
            "deletions": 25,
            "files": 5,
        }],
        "human_reviewed": [],
    }), encoding="utf-8")
    (data / "portfolio_cost.json").write_text(json.dumps({
        "total_30d_usd": 3.5,
        "by_recipe": {"morning_briefing": 1.25, "portfolio_health": 2.25},
        "trends": {"week_current_usd": 3.5},
    }), encoding="utf-8")
    (war / "ACTIVE_MISSION.md").write_text(
        "# Active Mission\n\n"
        "## Mission\n\nKeep cockpit parity visible.\n\n"
        "## Operating Rules\n\n- No external sends.\n- Record the run.\n",
        encoding="utf-8",
    )
    (war / "current-run.json").write_text(json.dumps({
        "run_id": "20260506-120000-cockpit-parity",
        "mission_id": "cockpit-parity",
        "updated_at": "2026-05-06T17:00:00Z",
        "path": str(war / "autonomy" / "runs" / "sample"),
    }), encoding="utf-8")
    (war / "NEXT_AGENT_CARD.md").write_text(
        "# Next Agent Card\n\n"
        "## Mission\n\nOperator Core: Cockpit mission control\n\n"
        "## Why This\n\nThe cockpit needs the current mission before retiring static files.\n\n"
        "## Start Here\n\n1. Read ACTIVE_MISSION.md.\n2. Read SOURCE_ACTIONS.md.\n\n"
        "## Verify\n\nRun cockpit tests.\n\n"
        "## Stop Rules\n\n- Stop before external sends.\n- Stop before destructive cleanup.\n",
        encoding="utf-8",
    )
    (war / "SOURCE_ACTIONS.md").write_text(
        "# Source Action Cards\n\n"
        "## Cards\n\n"
        "### 1. AI Ops Consulting / ao_waitlist\n\n"
        "- Product: **AI Ops Consulting**\n"
        "- Path: **waitlist**\n"
        "- Source: `ao_waitlist`\n"
        "- Status: **watch**\n"
        "- Issue: No rows yet.\n"
        "- First check: Inspect CTA visibility.\n"
        "- Likely fix: Make demand visible.\n"
        "- Verification: Refresh War Room.\n"
        "- Stop rule: Stop before live sends.\n",
        encoding="utf-8",
    )
    autonomy_missions = war / "autonomy" / "missions"
    autonomy_missions.mkdir(parents=True)
    (autonomy_missions / "active.json").write_text(json.dumps({
        "mission": {
            "id": "cockpit-parity",
            "title": "Cockpit Parity",
            "mode": "personal",
            "goal": "Keep war-room transition safe.",
            "success_criteria": ["Cockpit shows active mission."],
            "stop_conditions": ["Needs Kruz approval."],
        }
    }), encoding="utf-8")
    run_dir = war / "autonomy" / "runs" / "20260506-120000-cockpit-parity"
    run_dir.mkdir(parents=True)
    (war / "autonomy" / "run-index.jsonl").write_text(json.dumps({
        "run_id": "20260506-120000-cockpit-parity",
        "path": str(run_dir),
        "mission_id": "cockpit-parity",
        "updated_at": "2026-05-06T17:30:00Z",
    }) + "\n", encoding="utf-8")
    (run_dir / "status.json").write_text(json.dumps({
        "run_id": "20260506-120000-cockpit-parity",
        "mission_id": "cockpit-parity",
        "status": "active",
        "phase": "verify",
        "started_at": "2026-05-06T17:00:00Z",
        "updated_at": "2026-05-06T17:30:00Z",
        "horizon_hours": 6,
        "checkpoint_minutes": 30,
        "last_summary": "Mission Control connected.",
        "next_action": "Connect agent queue.",
        "score": {"verification": 4.0, "handoff": 5.0},
        "blocker": "",
    }), encoding="utf-8")
    (run_dir / "mission_snapshot.json").write_text(json.dumps({
        "mission": {
            "id": "cockpit-parity",
            "title": "Cockpit Parity",
            "goal": "Keep cockpit transition safe.",
            "verification_gates": ["Run cockpit tests."],
            "stop_conditions": ["Stop before external sends."],
        }
    }), encoding="utf-8")
    (run_dir / "checkpoint_000.md").write_text(
        "# Checkpoint 000\n\n"
        "- Time: 2026-05-06T17:10:00Z\n"
        "- Phase: build\n"
        "- Status: active\n\n"
        "## Summary\n\nStarted cockpit parity.\n\n"
        "## Evidence\n\n- Test fixture created.\n\n"
        "## Next\n\nKeep going.\n",
        encoding="utf-8",
    )
    (run_dir / "checkpoint_001.md").write_text(
        "# Checkpoint 001\n\n"
        "- Time: 2026-05-06T17:30:00Z\n"
        "- Phase: verify\n"
        "- Status: active\n\n"
        "## Summary\n\nMission Control connected.\n\n"
        "## Evidence\n\n- Cockpit tests pass.\n\n"
        "## Next\n\nConnect agent queue.\n",
        encoding="utf-8",
    )
    (run_dir / "evidence.jsonl").write_text(
        json.dumps({"ts": "2026-05-06T17:10:00Z", "type": "checkpoint_evidence", "phase": "build", "checkpoint": "checkpoint_000.md", "evidence": "Test fixture created."}) + "\n"
        + json.dumps({"ts": "2026-05-06T17:30:00Z", "type": "checkpoint_evidence", "phase": "verify", "checkpoint": "checkpoint_001.md", "evidence": "Cockpit tests pass."}) + "\n",
        encoding="utf-8",
    )
    (run_dir / "resume.md").write_text("# Resume Packet\n\n## Last Summary\n\nMission Control connected.\n", encoding="utf-8")
    (run_dir / "handoff_prompt.md").write_text("Continue the cockpit parity run.\n", encoding="utf-8")
    (war / "agent-launch-queue.json").write_text(json.dumps({
        "mode": "overnight-workbench",
        "purpose": "Turn War Room state into launchable agent missions.",
        "missions": [{
            "id": "build-source-registry",
            "title": "Build source registry",
            "best_agent": "Codex",
            "status": "ready",
            "duration_minutes": 45,
            "rank_score": 91,
            "autonomy_score": 80,
            "why": "Cockpit needs a migration map.",
            "output_artifact": "source registry",
            "resume_command": "Run cockpit tests.",
            "stop_rule": "Stop before external sends.",
            "verification": ["Registry renders."],
        }],
        "safety": ["Local-only work is allowed."],
    }), encoding="utf-8")
    (war / "agent-handoff-board.json").write_text(json.dumps({
        "mode": "parallel-agent-handoff",
        "purpose": "Coordinate agents without collisions.",
        "open_count": 1,
        "claimed_count": 0,
        "collision_count": 0,
        "missions": [{
            "id": "connect-agent-queue",
            "title": "Connect Agent Queue",
            "recommended_agent": "Codex",
            "priority": 88,
            "estimated_minutes": 60,
            "status": "open",
            "source": "AGENT_HANDOFF_BOARD.md",
            "why": "Cockpit needs the handoff board.",
            "output": "Agent Queue cockpit panel",
            "claim_file": "war-room/handoffs/CODEX.md",
            "write_scope": ["operator-core"],
            "read_first": ["war-room/agent-handoff-board.json"],
            "verification": ["Run tests."],
            "stop_rule": "Stop before destructive cleanup.",
        }],
        "claims": [
            {"agent": "CODEX", "path": "war-room/handoffs/CODEX.md", "status": "free", "mission_id": "", "started": "", "notes": "- Free."},
            {"agent": "CLAUDE", "path": "war-room/handoffs/CLAUDE.md", "status": "free", "mission_id": "", "started": "", "notes": "- Free."},
        ],
        "safety": ["Agents edit their own claim file first."],
    }), encoding="utf-8")
    handoffs = war / "handoffs"
    handoffs.mkdir()
    (handoffs / "OPEN_QUEUE.md").write_text("# Open Agent Handoff Queue\n\n## Connect Agent Queue\n\n- Mission ID: connect-agent-queue\n", encoding="utf-8")
    (handoffs / "CODEX.md").write_text(
        "# CODEX Handoff Slot\n\n"
        "- Status: free\n"
        "- Mission ID:\n"
        "- Agent: CODEX\n"
        "- Started:\n"
        "- Write scope:\n"
        "- Stop rule:\n\n"
        "## Notes\n\n- Free.\n",
        encoding="utf-8",
    )
    (handoffs / "CLAUDE.md").write_text(
        "# CLAUDE Handoff Slot\n\n"
        "- Status: free\n"
        "- Mission ID:\n"
        "- Agent: CLAUDE\n\n"
        "## Notes\n\n- Free.\n",
        encoding="utf-8",
    )
    (handoffs / "CLAIM_TEMPLATE.md").write_text("# Template\n\n- Status: free\n", encoding="utf-8")
    (war / "memory-os.json").write_text(json.dumps({
        "version": "memory-os-v1",
        "generated_at": "2026-05-06 12:00",
        "summary": {"indexed_documents": 3, "projects": 2, "decisions": 1},
        "pillars": [{"id": "memory_os", "name": "Kruz Memory OS", "role": "Brain", "purpose": "Keep durable context."}],
        "documents": [{
            "title": "War Room Run Log",
            "kind": "decision",
            "relative_path": "RUNLOG.md",
            "mtime": "2026-05-06 12:00",
            "signals": ["decision", "next_action"],
        }],
        "decisions": [{"title": "Current System Decisions", "preview": "Run Operator Core.", "source": "DECISION_JOURNAL.md", "mtime": "2026-05-06 12:00"}],
        "ask_workspace_prompts": ["What changed?"],
    }), encoding="utf-8")
    (war / "learning-loop.json").write_text(json.dumps({
        "mode": "learning-loop-v1",
        "purpose": "Replay runs and improve workflows.",
        "run_count": 2,
        "replay_count": 2,
        "latest_run": {
            "id": "run-2",
            "agent": "Codex",
            "project": "War Room",
            "mission": "Agent Handoff Board",
            "flow": "Product Sprint Flow",
            "grade": "A",
            "duration_minutes": 60,
            "universal_score": 11,
            "artifact_count": 4,
            "verification_count": 3,
            "verdict": "Repeat this flow.",
            "next_sprint": "Connect memory.",
        },
        "flow_patterns": [{
            "flow": "Product Sprint Flow",
            "run_count": 2,
            "average_score": 11,
            "average_duration_minutes": 30,
            "best_mission": "Agent Handoff Board",
            "best_grade": "A",
        }],
        "prediction_accuracy": {"known": 2, "matches": 1, "misses": 1, "accuracy_percent": 50.0},
        "next_learning_sprint": "Connect memory.",
    }), encoding="utf-8")
    (war / "FLOW_RECOMMENDATION.md").write_text(
        "# Flow Recommendation\n\n"
        "## Recommended Flow\n\n"
        "- Mission: **Operator Core: Cockpit mission control**\n"
        "- Flow: **Product Sprint Flow**\n"
        "- Confidence: **medium**\n"
        "- Historical runs for this flow: **2**\n"
        "- Average flow score: **11/12**\n\n"
        "## Why This Flow\n\n- Mission touches cockpit migration.\n\n"
        "## Run Contract\n\n- Required artifact: Product change plus test evidence.\n- Verification: Run tests.\n\n"
        "## Learning Rule\n\n- Record the run.\n",
        encoding="utf-8",
    )
    (war / "FLOW_LESSONS.md").write_text(
        "# Flow Lessons\n\n"
        "## What The War Room Has Learned\n\n- **Product Sprint Flow**: Reuse it when migration needs tests.\n\n"
        "## Best Observed Flow\n\n- **Product Sprint Flow** - Grade A.\n\n"
        "## Weakest Observed Flow\n\n- **Recovery Flow** - untried.\n\n"
        "## Next Experiment\n\n- Run the recommended flow and compare outcome.\n",
        encoding="utf-8",
    )
    (war / "FLOW_LIBRARY.md").write_text(
        "# Flow Library\n\n"
        "## Flow Scoreboard\n\n- **Product Sprint Flow** - Grade A, 11/12 flow score, 2 run(s).\n",
        encoding="utf-8",
    )
    (war / "DECISION_JOURNAL.md").write_text(
        "# Decision Journal\n\n"
        "## Current System Decisions\n\n"
        "- Decision: Run Operator Core cockpit migration.\n"
        "- Why: It preserves War Room momentum.\n"
        "- Would change if: A safety gate appears.\n"
        "- Revisit: next refresh.\n",
        encoding="utf-8",
    )
    (war / "real-agent-scoreboard.json").write_text(json.dumps({
        "date": "2026-05-06",
        "real_runs": [{
            "agent": "Codex",
            "workflow": "Product Sprint Flow",
            "mission": "Agent Handoff Board",
            "total": 10,
            "lesson": "Repeatable.",
            "source_run": "run-2",
        }],
    }), encoding="utf-8")
    (war / "project-motion-board.json").write_text(json.dumps({
        "mode": "project-motion-board",
        "purpose": "Watch projects move through lanes.",
        "lanes": [{"lane": "Building", "count": 1, "projects": ["operator-core"]}],
        "top_mover": {
            "id": "operator-core",
            "title": "Operator Core",
            "path": "operator-core",
            "lane": "Building",
            "temperature": "hot",
            "motion_score": 97,
            "mention_count": 3,
            "event_count": 2,
            "last_moved": "2026-05-06T17:40:00",
            "next_action": "Finish cockpit parity.",
            "evidence": "tests/test_cockpit_routes.py",
            "latest_run": {"mission": "Cockpit parity", "grade": "A", "duration_minutes": 60, "score": 12, "verdict": "Ship it."},
        },
        "projects": [{
            "id": "operator-core",
            "title": "Operator Core",
            "path": "operator-core",
            "lane": "Building",
            "temperature": "hot",
            "motion_score": 97,
            "mention_count": 3,
            "event_count": 2,
            "last_moved": "2026-05-06T17:40:00",
            "next_action": "Finish cockpit parity.",
            "evidence": "tests/test_cockpit_routes.py",
            "latest_run": {"mission": "Cockpit parity", "grade": "A", "duration_minutes": 60, "score": 12, "verdict": "Ship it."},
        }],
        "stream": [{"project": "Operator Core", "lane": "Building", "kind": "selected", "title": "Selected", "evidence": "test", "when": "now", "weight": 5}],
    }), encoding="utf-8")
    (war / "side-projects-portfolio-os.json").write_text(json.dumps({
        "mode": "side-projects-portfolio-os",
        "project_count": 2,
        "codex_count": 1,
        "claude_count": 1,
        "average_readiness": 92,
        "top_builds": ["23-tiktok-shop-watch"],
        "projects": [{
            "slug": "23-tiktok-shop-watch",
            "title": "TikTok Shop Watch",
            "owner": "Codex",
            "status": "built v0",
            "tier": "Tier 1",
            "buildScore": 99,
            "energyMode": "deep build",
            "nextAction": "Build product URL intake.",
            "tagline": "Watch products and sellers.",
            "readiness": {"score": 100, "grade": "A", "blockers": []},
            "git": {"clean": True},
        }],
        "safety": ["Local files only."],
    }), encoding="utf-8")
    (war / "SIDE_PROJECTS_NEXT_BUILDS.md").write_text(
        "# Side Projects Next Builds\n\n"
        "## 1. TikTok Shop Watch\n\n"
        "- Slug: 23-tiktok-shop-watch\n"
        "- Score: 99/100\n"
        "- Readiness: A / 100/100\n"
        "- First ticket: Build product URL intake.\n",
        encoding="utf-8",
    )
    (war / "NEXT_BUILD_CARD.md").write_text(
        "# Next Build Card\n\n"
        "## Build\n\n- **Personal Console follow-up**\n\n"
        "## Spec\n\nTurn this into a calmer command center.\n\n"
        "## Next Action\n\n- Build the smallest local artifact.\n\n"
        "## Stop Rule\n\n- Stop before external sends.\n\n"
        "## Safety\n\n- Local only.\n- No launch.\n",
        encoding="utf-8",
    )
    (war / "forge.json").write_text(json.dumps({
        "version": "forge-v2-run-ledger",
        "generated_at": "2026-05-06 12:00",
        "summary": {"proposal_count": 2, "ready_count": 1, "run_count": 1, "promoted_count": 0},
        "proposals": [{
            "id": "decision-to-build-promoter",
            "pillar": "Kruz Forge",
            "title": "Decision-To-Build Promoter",
            "status": "ready",
            "problem": "Decisions do not become build cards.",
            "build": ["Extract build-intent decisions."],
            "verification": ["Run classifier."],
            "scores": {"forge_score": 68},
        }],
        "runs": [{
            "id": "forge-run-1",
            "proposal_id": "decision-to-build-promoter",
            "project": "War Room",
            "status": "verified",
            "result": "Parsed proposal.",
            "updated_at": "2026-05-06 12:00",
            "verification": ["Parsed JSON."],
        }],
        "gate_library": [{"id": "local-only", "name": "Local only", "rule": "No external sends."}],
        "promotion_recommendation": {
            "proposal_id": "decision-to-build-promoter",
            "title": "Decision-To-Build Promoter",
            "why": "Highest score.",
            "first_action": "Extract build-intent decisions.",
            "verification": ["Run classifier."],
        },
    }), encoding="utf-8")
    (war / "forge-runs.jsonl").write_text(json.dumps({
        "id": "forge-run-jsonl",
        "proposal_id": "decision-to-build-promoter",
        "project": "War Room",
        "status": "verified",
        "result": "JSONL run parsed.",
        "updated_at": "2026-05-06 12:10",
        "verification": ["Parsed JSONL."],
    }) + "\n", encoding="utf-8")
    (war / "kruz-skills.json").write_text(json.dumps({
        "version": "2026-05-06",
        "purpose": "Local workflow skills.",
        "principles": ["Evidence exists."],
        "skills": [{
            "id": "war-room-polish",
            "name": "War Room Polish",
            "domain": "both",
            "use_when": "Improving cockpit.",
            "triggers": ["cockpit", "dashboard"],
        }],
    }), encoding="utf-8")
    (war / "kruz-skill-runs.jsonl").write_text(json.dumps({
        "timestamp": "2026-05-06T17:40:00",
        "agent": "Codex",
        "skill_id": "war-room-polish",
        "task": "Cockpit parity",
        "total": 28,
        "max": 30,
        "lesson": "Parity needs registry evidence.",
    }) + "\n", encoding="utf-8")
    (war / "kruz-skill-proposals.jsonl").write_text(json.dumps({
        "id": "better-cockpit-audit",
        "skill_id": "war-room-polish",
        "proposal": "Add cockpit parity checks.",
    }) + "\n", encoding="utf-8")
    (war / "all-pages-qa.json").write_text(json.dumps({
        "status": "pass",
        "page_count": 2,
        "pass_count": 2,
        "warn_count": 0,
        "missing_count": 0,
        "browser_qa_status": "ready-for-browser",
        "pages": [{"id": "cockpit", "title": "Cockpit", "status": "pass", "file": "cockpit.html"}],
    }), encoding="utf-8")
    (war / "gauntlet-report.json").write_text(json.dumps({
        "date": "2026-05-06",
        "mission": "Cockpit parity gauntlet",
        "verdict": "pass",
        "lesson": "Keep migration local and testable.",
        "next_experiment": "Connect remaining sources.",
        "flow_score": {"total": 12, "max": 12},
        "universal_score": {"total": 12, "max": 12},
    }), encoding="utf-8")
    (war / "agent-runs.jsonl").write_text(json.dumps({
        "id": "run-history-1",
        "date": "2026-05-06",
        "agent": "Codex",
        "project": "Operator Core",
        "mission": "Cockpit parity",
        "verdict": "Ship cockpit parity.",
        "next_sprint": "Keep moving.",
        "scores": {"total": 11, "max": 12},
    }) + "\n", encoding="utf-8")
    (war / "RUNLOG.md").write_text("# War Room Run Log\n\n## Cockpit parity\n", encoding="utf-8")
    evaluations = war / "evaluations"
    evaluations.mkdir()
    (evaluations / "cockpit-parity.md").write_text("# Cockpit Parity Evaluation\n\nPass.\n", encoding="utf-8")
    cockpit_dir = war / "cockpit"
    cockpit_dir.mkdir()
    (cockpit_dir / "cockpit_app.py").write_text("print('streamlit cockpit')\n", encoding="utf-8")
    (cockpit_dir / "cockpit_state.py").write_text("STATE = {}\n", encoding="utf-8")
    return {"data": data, "status": status, "war": war}


def test_collect_cockpit_state_reads_artifacts(cockpit_env):
    state = collect_cockpit_state()
    assert state["statuses"]["count"] == 2
    assert state["statuses"]["health_counts"]["red"] == 1
    assert state["portfolio"]["overview"]["Tracked"] == 2
    assert state["weekly_review"]["auto_merged"][0]["title"] == "sample no-review merge"
    assert state["cost"]["by_recipe"]["portfolio_health"] == 2.25
    assert state["source_registry"]["summary"]["connected"] >= 5
    assert not any(item["id"] == "active-mission" for item in state["source_registry"]["missing_connections"])
    assert state["mission_control"]["mission"]["title"] == "Operator Core: Cockpit mission control"
    assert state["mission_control"]["source_actions"]["count"] == 1
    assert state["mission_control"]["current_run"]["run_id"] == "20260506-120000-cockpit-parity"
    assert state["agent_coordination"]["launch_queue"]["top_mission"]["title"] == "Build source registry"
    assert state["agent_coordination"]["handoff_board"]["open_count"] == 1
    assert state["autonomy_evidence"]["latest"]["run_id"] == "20260506-120000-cockpit-parity"
    assert state["autonomy_evidence"]["latest"]["checkpoint_count"] == 2
    assert state["memory_learning"]["flow_recommendation"]["flow"] == "Product Sprint Flow"
    assert state["memory_learning"]["learning"]["latest_run"]["mission"] == "Agent Handoff Board"
    assert state["portfolio_motion"]["project_motion"]["top_mover"]["title"] == "Operator Core"
    assert state["portfolio_motion"]["side_projects"]["projects"][0]["title"] == "TikTok Shop Watch"
    assert state["portfolio_motion"]["forge"]["promotion_recommendation"]["title"] == "Decision-To-Build Promoter"
    assert state["quality_history"]["skills"]["skill_count"] == 1
    assert state["quality_history"]["qa"]["all_pages"]["status"] == "pass"
    assert state["quality_history"]["run_history"]["run_count"] == 1
    assert state["project_timeline"]["summary"]["event_count"] >= 8
    assert state["project_timeline"]["summary"]["counts_by_type"]["status_snapshot"] == 2
    assert "operator-core" in state["project_timeline"]["by_project"]
    assert (cockpit_env["data"] / "project_timelines" / "operator-core.jsonl").exists()


def test_collect_mission_control_reads_mission_artifacts(cockpit_env):
    mission = collect_mission_control(cockpit_env["war"])
    assert mission["mission"]["active_summary"] == "Keep cockpit parity visible."
    assert mission["next_agent"]["start_here"] == ["Read ACTIVE_MISSION.md.", "Read SOURCE_ACTIONS.md."]
    assert mission["next_agent"]["stop_rules"] == ["Stop before external sends.", "Stop before destructive cleanup."]
    assert mission["source_actions"]["cards"][0]["product"] == "AI Ops Consulting"
    assert mission["source_actions"]["cards"][0]["source"] == "ao_waitlist"


def test_collect_agent_coordination_reads_launch_and_handoff(cockpit_env):
    agents = collect_agent_coordination(cockpit_env["war"])
    assert agents["launch_queue"]["mission_count"] == 1
    assert agents["launch_queue"]["top_mission"]["agent"] == "Codex"
    assert agents["handoff_board"]["missions"][0]["title"] == "Connect Agent Queue"
    assert agents["handoff_board"]["claims"][0]["agent"] == "CLAUDE"
    assert agents["handoff_board"]["claims"][1]["agent"] == "CODEX"
    assert "CLAIM_TEMPLATE" not in {claim["agent"] for claim in agents["handoff_board"]["claims"]}
    assert agents["open_queue"]["preview"].startswith("# Open Agent Handoff Queue")


def test_collect_autonomy_evidence_reads_latest_run(cockpit_env):
    autonomy = collect_autonomy_evidence(cockpit_env["war"])
    latest = autonomy["latest"]
    assert autonomy["run_count"] == 1
    assert latest["mission_title"] == "Cockpit Parity"
    assert latest["phase"] == "verify"
    assert latest["checkpoint_count"] == 2
    assert latest["latest_checkpoint"]["name"] == "checkpoint_001.md"
    assert latest["latest_checkpoint"]["evidence"] == ["Cockpit tests pass."]
    assert latest["evidence_tail"][-1]["evidence"] == "Cockpit tests pass."
    assert "Resume Packet" in latest["resume_preview"]


def test_collect_memory_learning_reads_memory_and_flow(cockpit_env):
    memory = collect_memory_learning(cockpit_env["war"])
    assert memory["memory"]["summary"]["indexed_documents"] == 3
    assert memory["memory"]["recent_documents"][0]["title"] == "War Room Run Log"
    assert memory["learning"]["latest_run"]["mission"] == "Agent Handoff Board"
    assert memory["flow_recommendation"]["flow"] == "Product Sprint Flow"
    assert memory["decisions"][0]["decision"] == "Run Operator Core cockpit migration."
    assert memory["scoreboard"]["real_runs"][0]["total"] == 10


def test_collect_portfolio_motion_reads_motion_side_projects_and_forge(cockpit_env):
    motion = collect_portfolio_motion(cockpit_env["war"])
    assert motion["project_motion"]["top_mover"]["motion_score"] == 97
    assert motion["project_motion"]["projects"][0]["title"] == "Operator Core"
    assert motion["side_projects"]["project_count"] == 2
    assert motion["side_projects"]["projects"][0]["slug"] == "23-tiktok-shop-watch"
    assert motion["side_next_builds"][0]["title"] == "TikTok Shop Watch"
    assert motion["next_build_card"]["build"] == "Personal Console follow-up"
    assert motion["forge"]["ready_proposals"][0]["title"] == "Decision-To-Build Promoter"
    assert motion["forge"]["runs"][0]["id"] == "forge-run-jsonl"


def test_collect_quality_history_reads_skills_qa_and_runs(cockpit_env):
    quality = collect_quality_history(cockpit_env["war"])
    assert quality["skills"]["skill_count"] == 1
    assert quality["skills"]["latest_runs"][0]["total"] == 28
    assert quality["qa"]["all_pages"]["pass_count"] == 2
    assert quality["qa"]["gauntlet"]["verdict"] == "pass"
    assert quality["qa"]["evaluation_files"][0]["title"] == "Cockpit Parity Evaluation"
    assert quality["run_history"]["latest_runs"][0]["mission"] == "Cockpit parity"


def test_source_registry_tracks_unconnected_and_missing_sources(cockpit_env):
    registry = collect_source_registry(
        war_room_dir=cockpit_env["war"],
        data_dir=cockpit_env["data"],
        status_dir=cockpit_env["status"],
        repo_root=Path(__file__).resolve().parents[1],
    )

    by_id = {item["id"]: item for item in registry["items"]}
    assert by_id["status-docs"]["connection"] == "connected"
    assert by_id["status-docs"]["health"] == "ok"
    assert by_id["active-mission"]["connection"] == "connected"
    assert by_id["active-mission"]["health"] == "ok"
    assert by_id["next-agent-card"]["connection"] == "connected"
    assert by_id["agent-launch-queue"]["connection"] == "connected"
    assert by_id["agent-handoff-board"]["connection"] == "connected"
    assert by_id["autonomy-runs"]["connection"] == "connected"
    assert by_id["memory-os"]["connection"] == "connected"
    assert by_id["learning-loop"]["connection"] == "connected"
    assert by_id["project-motion"]["connection"] == "connected"
    assert by_id["side-project-os"]["connection"] == "connected"
    assert by_id["forge"]["connection"] == "connected"
    assert by_id["skills-arena"]["connection"] == "connected"
    assert by_id["evaluations"]["connection"] == "connected"
    assert by_id["run-history"]["connection"] == "connected"
    assert by_id["agent-launch-queue"]["exists"] is True
    assert by_id["forge"]["health"] == "ok"
    assert by_id["streamlit-cockpit"]["connection"] == "static-only"


def test_source_registry_marks_stale_sources(cockpit_env):
    old = time.time() - (96 * 3600)
    for old_path in [
        cockpit_env["war"] / "ACTIVE_MISSION.md",
        cockpit_env["war"] / "current-run.json",
        cockpit_env["war"] / "autonomy" / "missions" / "active.json",
    ]:
        old_path.touch()
        os.utime(old_path, (old, old))

    registry = collect_source_registry(
        war_room_dir=cockpit_env["war"],
        data_dir=cockpit_env["data"],
        status_dir=cockpit_env["status"],
        repo_root=Path(__file__).resolve().parents[1],
    )
    active = next(item for item in registry["items"] if item["id"] == "active-mission")
    assert active["health"] == "stale"
    assert active["stale"] is True


def test_cockpit_routes_render_html_and_json(cockpit_env, tmp_path):
    saved = dict(EXTRA_ROUTES)
    EXTRA_ROUTES.clear()
    try:
        register_cockpit_routes()
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        server = OperatorHttpServer(("127.0.0.1", port), JobStore(tmp_path / "jobs.sqlite"), status_path=tmp_path / "status.json")
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/cockpit")
            resp = conn.getresponse()
            html = resp.read().decode("utf-8")
            conn.close()
            assert resp.status == 200
            assert "Operator Cockpit" in html
            assert "sample no-review merge" in html
            assert "Morning Briefing" in html
            assert "War-room Source Registry" in html
            assert "Active mission" in html
            assert "Mission Control" in html
            assert "Operator Core: Cockpit mission control" in html
            assert "AI Ops Consulting" in html
            assert "Agent Queue / Handoff" in html
            assert "Build source registry" in html
            assert "Connect Agent Queue" in html
            assert "Autonomy Evidence" in html
            assert "Mission Control connected" in html
            assert "Cockpit tests pass" in html
            assert "Memory + Learning" in html
            assert "Product Sprint Flow" in html
            assert "Run Operator Core cockpit migration" in html
            assert "Portfolio Motion Command" in html
            assert "Finish cockpit parity" in html
            assert "TikTok Shop Watch" in html
            assert "Decision-To-Build Promoter" in html
            assert "Skills / QA / Run History" in html
            assert "War Room Polish" in html
            assert "Keep migration local and testable" in html
            assert "Action Packets" in html
            assert "Create Local Packet" in html
            assert "Packet Audit" in html
            assert "Project Timeline" in html
            assert "Action Queue" in html
            assert "agent_checkpoint" in html

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/cockpit.json")
            resp = conn.getresponse()
            data = json.loads(resp.read())
            conn.close()
            assert resp.status == 200
            assert data["statuses"]["health_counts"]["green"] == 1
            assert data["source_registry"]["summary"]["not_connected"] == 0
            assert data["mission_control"]["source_actions"]["count"] == 1
            assert data["agent_coordination"]["handoff_board"]["open_count"] == 1
            assert data["autonomy_evidence"]["latest"]["checkpoint_count"] == 2
            assert data["memory_learning"]["memory"]["summary"]["indexed_documents"] == 3
            assert data["portfolio_motion"]["project_motion"]["top_mover"]["title"] == "Operator Core"
            assert data["quality_history"]["qa"]["all_pages"]["status"] == "pass"
            assert data["action_packets"]["summary"]["count"] == 0
            assert data["project_timeline"]["summary"]["counts_by_type"]["agent_checkpoint"] == 2
            assert data["project_timeline"]["summary"]["actionable_count"] >= 1
            assert data["project_timeline"]["action_queue"]

            event_id = next(
                event["id"]
                for event in data["project_timeline"]["action_queue"]
                if event["type"] == "pr_merged_no_review"
            )

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            payload = json.dumps({"event_id": event_id})
            conn.request(
                "POST",
                "/cockpit/timeline/create-packet",
                body=payload,
                headers={"Content-Type": "application/json"},
            )
            resp = conn.getresponse()
            from_event = json.loads(resp.read())
            conn.close()
            assert resp.status == 201
            event_packet_id = from_event["packet"]["id"]
            assert from_event["packet"]["kind"] == "weekly_review_follow_up"
            assert from_event["packet"]["status"] == "ready"
            assert from_event["packet"]["context"]["source_event"]["id"] == event_id

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            payload = json.dumps({"event_id": event_id})
            conn.request(
                "POST",
                "/cockpit/timeline/create-packet",
                body=payload,
                headers={"Content-Type": "application/json"},
            )
            resp = conn.getresponse()
            duplicate = json.loads(resp.read())
            conn.close()
            assert resp.status == 200
            assert duplicate["deduped"] is True
            assert duplicate["packet"]["id"] == event_packet_id

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            payload = json.dumps({
                "kind": "weekly_review_follow_up",
                "title": "Review autonomous merges",
                "context_summary": "Spot-check largest no-review merge.",
            })
            conn.request(
                "POST",
                "/cockpit/actions/create",
                body=payload,
                headers={"Content-Type": "application/json"},
            )
            resp = conn.getresponse()
            created = json.loads(resp.read())
            conn.close()
            assert resp.status == 201
            packet_id = created["packet"]["id"]
            assert created["packet"]["status"] == "draft"
            assert (cockpit_env["data"] / "action_packets" / f"{packet_id}.md").exists()

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            payload = json.dumps({"id": packet_id, "status": "ready"})
            conn.request(
                "POST",
                "/cockpit/actions/status",
                body=payload,
                headers={"Content-Type": "application/json"},
            )
            resp = conn.getresponse()
            updated = json.loads(resp.read())
            conn.close()
            assert resp.status == 200
            assert updated["packet"]["status"] == "ready"

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            payload = json.dumps({"id": packet_id, "status": "claimed", "actor": "codex", "note": "route test claim"})
            conn.request(
                "POST",
                "/cockpit/actions/status",
                body=payload,
                headers={"Content-Type": "application/json"},
            )
            resp = conn.getresponse()
            claimed = json.loads(resp.read())
            conn.close()
            assert resp.status == 200
            assert claimed["packet"]["status"] == "claimed"
            assert claimed["packet"]["claimed_by"] == "codex"

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            payload = json.dumps({"id": packet_id, "status": "done", "actor": "codex", "note": "route test done"})
            conn.request(
                "POST",
                "/cockpit/actions/status",
                body=payload,
                headers={"Content-Type": "application/json"},
            )
            resp = conn.getresponse()
            done = json.loads(resp.read())
            conn.close()
            assert resp.status == 200
            assert done["packet"]["status"] == "done"
            assert done["packet"]["done_at"]

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/cockpit.json")
            resp = conn.getresponse()
            data = json.loads(resp.read())
            conn.close()
            assert data["action_packets"]["summary"]["count"] == 2
            assert data["action_packets"]["summary"]["by_status"]["ready"] == 1
            assert data["action_packets"]["summary"]["by_status"]["done"] == 1
            assert len(data["action_packets"]["audit"]) >= 5
            assert any(
                event["type"] == "action_packet" and event["payload"]["packet_id"] == packet_id
                for event in data["project_timeline"]["latest"]
            )
            assert any(
                event["type"] == "action_packet" and event["payload"]["packet_id"] == event_packet_id
                for event in data["project_timeline"]["latest"]
            )

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/cockpit/project?project=operator-core")
            resp = conn.getresponse()
            project_html = resp.read().decode("utf-8")
            conn.close()
            assert resp.status == 200
            assert "operator-core Timeline" in project_html
            assert "Linked Packets" in project_html

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/cockpit/project.json?project=operator-core")
            resp = conn.getresponse()
            project_data = json.loads(resp.read())
            conn.close()
            assert resp.status == 200
            assert project_data["project"] == "operator-core"
            assert project_data["summary"]["event_count"] >= 1
            assert any(packet["id"] == event_packet_id for packet in project_data["packets"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
    finally:
        EXTRA_ROUTES.clear()
        EXTRA_ROUTES.update(saved)
