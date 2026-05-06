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
    assert by_id["agent-launch-queue"]["exists"] is True
    assert by_id["forge"]["health"] == "missing"
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

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/cockpit.json")
            resp = conn.getresponse()
            data = json.loads(resp.read())
            conn.close()
            assert resp.status == 200
            assert data["statuses"]["health_counts"]["green"] == 1
            assert data["source_registry"]["summary"]["not_connected"] > 0
            assert data["mission_control"]["source_actions"]["count"] == 1
            assert data["agent_coordination"]["handoff_board"]["open_count"] == 1
            assert data["autonomy_evidence"]["latest"]["checkpoint_count"] == 2
            assert data["memory_learning"]["memory"]["summary"]["indexed_documents"] == 3
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
    finally:
        EXTRA_ROUTES.clear()
        EXTRA_ROUTES.update(saved)
